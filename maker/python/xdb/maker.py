# Copyright 2022 The Ip2Region Authors. All rights reserved.
# Use of this source code is governed by a Apache2.0-style
# license that can be found in the LICENSE file.
#
# Author: linyufeng <leolin49@foxmail.com>
# Date  : 2022/7/14 17:00
#
# ----
# ip2region database v2.0 structure
#
# +----------------+-------------------+---------------+--------------+
# | header space   | speed up index    |  data payload | block index  |
# +----------------+-------------------+---------------+--------------+
# | 256 bytes      | 512 KiB (fixed)   | dynamic size  | dynamic size |
# +----------------+-------------------+---------------+--------------+
#
# 1. padding space : for header info like block index ptr, version, release date eg ... or any other temporary needs.
# -- 2bytes: version number, different version means structure update, it fixed to 2 for now
# -- 2bytes: index algorithm code.
# -- 4bytes: generate unix timestamp (version)
# -- 4bytes: index block start ptr
# -- 4bytes: index block end ptr
#
#
# 2. data block : region or whatever data info.
# 3. segment index block : binary index block.
# 4. vector index block  : fixed index info for block index search speed up.
# space structure table:
# -- 0   -> | 1rt super block | 2nd super block | 3rd super block | ... | 255th super block
# -- 1   -> | 1rt super block | 2nd super block | 3rd super block | ... | 255th super block
# -- 2   -> | 1rt super block | 2nd super block | 3rd super block | ... | 255th super block
# -- ...
# -- 255 -> | 1rt super block | 2nd super block | 3rd super block | ... | 255th super block
#
#
# super block structure:
# +-----------------------+----------------------+
# | first index block ptr | last index block ptr |
# +-----------------------+----------------------+
#
# data entry structure:
# +--------------------+-----------------------+
# | 2bytes (for desc)  |  dynamic length	   |
# +--------------------+-----------------------+
#  data length   whatever in bytes
#
# index entry structure
# +------------+-----------+---------------+------------+
# | 4bytes	   | 4bytes	   | 2bytes		   | 4 bytes    |
# +------------+-----------+---------------+------------+
#  start ip 	  end ip	  data length     data ptr
import logging
import struct
import time
import sys

import xdb.segment as seg
import xdb.index as idx
import xdb.util as util


Version_No = 2
Header_Info_Length = 256
Vector_Index_Rows = 256
Vector_Index_Cols = 256
Vector_Index_Size = 8
Vector_Index_Length = Vector_Index_Rows * Vector_Index_Cols * Vector_Index_Size


class Maker:
    src_handle = None
    dst_handle = None
    index_policy = idx.Vector_Index_Policy
    segments = None
    region_pool = None
    vector_index = None

    def __init__(self, sh, dh, ip, sg, rp, vi):
        self.src_handle = sh
        self.dst_handle = dh
        self.index_policy = ip
        self.segments = sg
        self.region_pool = rp
        self.vector_index = vi

    def init(self):
        """
        Init the `xdb` binary file.
        1. Init the file header
        2. Load all the segments
        """
        self.init_db_header()
        self.load_segments()

    def init_db_header(self):
        """
        Init and write the file header to the destination xdb file.
        """
        logging.info("try to init the db header ... ")
        self.src_handle.seek(0, 0)

        # Make and write the header space
        header = bytearray([0] * 256)
        # 1. Version number
        header[0:2] = Version_No.to_bytes(2, byteorder="little")
        # 2. Index policy code
        header[2:4] = int(self.index_policy).to_bytes(2, byteorder="little")
        # 3. Generate unix timestamp
        header[4:8] = int(time.time()).to_bytes(4, byteorder="little")
        # 4. Index block start ptr
        header[8:12] = int(0).to_bytes(4, byteorder="little")
        # 5. Index block end ptr
        header[12:16] = int(0).to_bytes(4, byteorder="little")
        # Write header buffer to file
        self.dst_handle.write(header)

    def load_segments(self) -> list:
        """
        Load the segments [start ip|end ip|region] from source ip text file.
        :return: the list of Segment
        """
        logging.info("try to load the segments ... ")
        last = None
        s_tm = time.time()

        lines = self.src_handle.read().splitlines()
        for line in lines:
            logging.info(f"load segment: `{line}`")
            ps = line.split("|", maxsplit=2)
            if len(ps) != 3:
                logging.error(f"invalid ip segment line `{line}`")
                return []
            sip = util.check_ip(ps[0])
            if sip == -1:
                logging.error(f"invalid ip address `{ps[0]}` in line `{line}`")
                return []
            eip = util.check_ip(ps[1])
            if eip == -1:
                logging.error(f"invalid ip address `{ps[1]}` in line `{line}`")
                return []
            if sip > eip:
                logging.error(f"start ip({ps[0]}) should not be greater than end ip({ps[1]})")
                return []
            if len(ps[2]) < 1:
                logging.error(f"empty region info in segment line `{line}`")
                return []

            segment = seg.Segment(sip=sip, eip=eip, reg=ps[2])
            # Check the continuity of data segment
            if last is not None and last.end_ip + 1 != segment.start_ip:
                logging.error(
                    f"discontinuous data segment: last.eip+1({sip})!=seg.sip({eip}, {ps[0]})"
                )

                return []
            self.segments.append(segment)
            last = segment
        logging.info(
            f"all segments loaded, length: {len(self.segments)}, elapsed: {time.time() - s_tm}"
        )

    def set_vector_index(self, ip, ptr):
        """
        Init and refresh the vector index based on the IP pre-two bytes.
        """
        row, col = (ip >> 24) & 0xFF, (ip >> 16) & 0xFF
        vi_block = self.vector_index[row][col]
        if vi_block.first_ptr == 0:
            vi_block.first_ptr = ptr
        vi_block.last_ptr = ptr + idx.Segment_Index_Block_Size
        self.vector_index[row][col] = vi_block

    def start(self):
        """
        Start to make the 'xdb' binary file.
        """
        if len(self.segments) < 1:
            logging.error("empty segment list")
            return

        # 1. Write all the region/data to the binary file
        self.dst_handle.seek(Header_Info_Length + Vector_Index_Length, 0)

        logging.info("try to write the data block ... ")
        for s in self.segments:
            logging.info(f"try to write region '{s.region}'...")
            if s.region in self.region_pool:
                logging.info(f" --[Cached] with ptr={self.region_pool[s.region]}")
                continue
            region = bytes(s.region, encoding="utf-8")
            if len(region) > 0xFFFF:
                logging.error(
                    f"too long region info `{s.region}`: should be less than 65535 bytes"
                )

                return
            # Get the first ptr of the next region
            pos = self.dst_handle.seek(0, 1)
            logging.info(f"{pos} {region} {s.region}")
            self.dst_handle.write(region)
            self.region_pool[s.region] = pos
            logging.info(f" --[Added] with ptr={pos}")
        # 2. Write the index block and cache the super index block
        logging.info("try to write the segment index block ... ")
        counter, start_index_ptr, end_index_ptr = 0, -1, -1
        for sg in self.segments:
            if sg.region not in self.region_pool:
                logging.error(f"missing ptr cache for region `{sg.region}`")
                return
            data_len = len(bytes(sg.region, encoding="utf-8"))
            if data_len < 1:
                logging.error(f"empty region info for segment '{sg.region}'")
                return

            seg_list = sg.split()
            logging.info(f"try to index segment({len(seg_list)} split) {sg} ...")
            for s in seg_list:
                pos = self.dst_handle.seek(0, 1)

                s_index = idx.SegmentIndexBlock(
                    sip=s.start_ip,
                    eip=s.end_ip,
                    dl=data_len,
                    dp=self.region_pool[sg.region],
                )
                self.dst_handle.write(s_index.encode())
                logging.info(f"|-segment index: {counter}, ptr: {pos}, segment: {s}")
                self.set_vector_index(s.start_ip, pos)
                counter += 1

                # Check and record the start index ptr
                if start_index_ptr == -1:
                    start_index_ptr = pos
                end_index_ptr = pos

        # 3. Synchronized the vector index block
        logging.info("try to write the vector index block ... ")
        self.dst_handle.seek(Header_Info_Length, 0)
        for i in range(len(self.vector_index)):
            for j in range(len(self.vector_index[i])):
                vi = self.vector_index[i][j]
                self.dst_handle.write(vi.encode())

        # 4. Synchronized the segment index info
        logging.info("try to write the segment index ptr ... ")
        buff = struct.pack("<II", start_index_ptr, end_index_ptr)
        self.dst_handle.seek(8, 0)
        self.dst_handle.write(buff)

        logging.info(
            f"write done, dataBlocks: {len(self.region_pool)}, indexBlocks: ({len(self.segments)}, {counter}), indexPtr: ({start_index_ptr}, {end_index_ptr})"
        )

    def end(self):
        """
        End of make the 'xdb' binary file.
        """
        try:
            self.src_handle.close()
            self.dst_handle.close()
        except IOError as e:
            logging.error(e)
            sys.exit()


def new_maker(policy: int, srcfile: str, dstfile: str) -> Maker:
    """
    Create a xdb Maker to make the xdb binary file
    :param policy: index algorithm code 1:vector, 2:b-tree
    :param srcfile: source ip text file path
    :param dstfile: destination binary xdb file path
    :return: the 'xdb' Maker
    """
    try:
        sh = open(srcfile, mode="r", encoding="utf-8")
        dh = open(dstfile, mode="wb")
        return Maker(
            sh=sh,
            dh=dh,
            ip=policy,
            sg=[],
            rp={},
            vi=[
                [idx.VectorIndexBlock() for _ in range(Vector_Index_Rows)]
                for _ in range(Vector_Index_Cols)
            ],
        )
    except IOError as e:
        logging.error(e)
        sys.exit()
