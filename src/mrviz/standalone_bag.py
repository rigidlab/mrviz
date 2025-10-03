"""
Standalone ROS bag parser for reading and deserializing ROS1 .bag files without ROS dependencies.

Features:
- Supports reading messages from compressed (none, bz2, lz4) and uncompressed bag files.
- Deserializes common ROS message types (geometry_msgs, nav_msgs, sensor_msgs, etc.).
- Handles primitive types, arrays, nested messages, and custom message definitions.
- Provides utilities for inspecting topics, message counts, and connection info.
- Gracefully handles buffer underruns and corrupted data by returning raw binary or placeholders.
- Implements a generator interface for efficient, memory-friendly message iteration.

Usage:
    from amrviz.bag import Bag
    with Bag("your.bag") as bag:
        for topic, msg, timestamp in bag.read_messages(topics=['/your_topic']):
            print(topic, timestamp, msg)

Classes:
    - StandaloneRosbagParser: Main parser class (aliased as Bag).
    - ROSTime, Header, Quaternion, Point, Vector3, Pose, etc.: Common ROS message type representations.

Exceptions:
    - BufferUnderrunError: Raised when attempting to read beyond buffer bounds.
"""

import struct, bz2, lz4.frame, re, logging
from io import BytesIO
from typing import Dict, List, Tuple, Any, Optional, Union

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("standalone_rosbag_parser")

OP_MSG = 0x02
OP_CONNECTION = 0x07
OP_CHUNK = 0x05

_TYPE_MAP = {
    'float32': ('f', 4), 'float64': ('d', 8),
    'int8': ('b', 1), 'uint8': ('B', 1),
    'int16': ('h', 2), 'uint16': ('H', 2),
    'int32': ('i', 4), 'uint32': ('I', 4),
    'int64': ('q', 8), 'uint64': ('Q', 8),
    'bool': ('?', 1), 'char': ('B', 1), 'byte': ('b', 1),
    'time': ('II', 8), 'duration': ('ii', 8),
    'string': None,
}

# Extended type registry for common ROS message types
class ROSTime:
    """Standalone ROS Time representation"""
    __slots__ = ['secs', 'nsecs']
    _slot_types = ['uint32', 'uint32']
    
    def __init__(self, secs=0, nsecs=0):
        self.secs = secs
        self.nsecs = nsecs
    
    def to_sec(self):
        return self.secs + self.nsecs * 1e-9
    
    def __repr__(self):
        return f"ROSTime(secs={self.secs}, nsecs={self.nsecs})"

class Header: 
    __slots__ = ['seq', 'stamp', 'frame_id']
    _slot_types = ['uint32', 'time', 'string']

class Quaternion: 
    __slots__ = ['x','y','z','w']
    _slot_types = ['float64']*4

class Point: 
    __slots__ = ['x','y','z']
    _slot_types = ['float64']*3

class Vector3:
    __slots__ = ['x','y','z']
    _slot_types = ['float64']*3

class Pose: 
    __slots__ = ['position','orientation']
    _slot_types = ['geometry_msgs/Point','geometry_msgs/Quaternion']

class PoseWithCovariance:
    __slots__ = ['pose','covariance']
    _slot_types = ['geometry_msgs/Pose','float64[36]']

class Twist:
    __slots__ = ['linear','angular']
    _slot_types = ['geometry_msgs/Vector3','geometry_msgs/Vector3']

class TwistWithCovariance:
    __slots__ = ['twist','covariance']
    _slot_types = ['geometry_msgs/Twist','float64[36]']

class Odometry:
    __slots__ = ['header','child_frame_id','pose','twist']
    _slot_types = ['std_msgs/Header','string','geometry_msgs/PoseWithCovariance','geometry_msgs/TwistWithCovariance']

class MapMetaData: 
    __slots__ = ['map_load_time','resolution','width','height','origin']
    _slot_types = ['time','float32','uint32','uint32','geometry_msgs/Pose']

class OccupancyGrid: 
    __slots__ = ['header','info','data']
    _slot_types = ['std_msgs/Header','nav_msgs/MapMetaData','int8[]']

class CompressedImage:
    __slots__ = ['header','format','data']
    _slot_types = ['std_msgs/Header','string','uint8[]']

class Image:
    __slots__ = ['header','height','width','encoding','is_bigendian','step','data']
    _slot_types = ['std_msgs/Header','uint32','uint32','string','uint8','uint32','uint8[]']

class LaserScan:
    __slots__ = ['header','angle_min','angle_max','angle_increment','time_increment','scan_time','range_min','range_max','ranges','intensities']
    _slot_types = ['std_msgs/Header','float32','float32','float32','float32','float32','float32','float32','float32[]','float32[]']

class PointField:
    __slots__ = ['name','offset','datatype','count']
    _slot_types = ['string','uint32','uint8','uint32']

class PointCloud2:
    __slots__ = ['header','height','width','fields','is_bigendian','point_step','row_step','data','is_dense']
    _slot_types = ['std_msgs/Header','uint32','uint32','sensor_msgs/PointField[]','bool','uint32','uint32','uint8[]','bool']

class Voxel:
    __slots__ = ['x', 'y', 'z', 'value']
    _slot_types = ['float32', 'float32', 'float32', 'float32']

# Extended type registry
_ros_type_map = {
    'time': ROSTime,
    'std_msgs/Header': Header,
    'geometry_msgs/Quaternion': Quaternion,
    'geometry_msgs/Point': Point,
    'geometry_msgs/Vector3': Vector3,
    'geometry_msgs/Pose': Pose,
    'geometry_msgs/PoseWithCovariance': PoseWithCovariance,
    'geometry_msgs/Twist': Twist,
    'geometry_msgs/TwistWithCovariance': TwistWithCovariance,
    'nav_msgs/Odometry': Odometry,
    'nav_msgs/MapMetaData': MapMetaData,
    'nav_msgs/OccupancyGrid': OccupancyGrid,
    'sensor_msgs/CompressedImage': CompressedImage,
    'sensor_msgs/Image': Image,
    'sensor_msgs/LaserScan': LaserScan,
    'sensor_msgs/PointField': PointField,
    'sensor_msgs/PointCloud2': PointCloud2,
    'Voxel': Voxel,
}

def parse_all_msgs(msg_def: str) -> Dict[str, List[Tuple[str, str, bool]]]:
    """Parse message definition into type definitions."""
    blocks = msg_def.strip().split('===')[1:]
    all_defs = {}
    
    for part in [msg_def] + blocks:
        lines = [l.strip() for l in part.strip().splitlines() if l and not l.startswith('#')]
        typedef = []
        typename = None
        
        for line in lines:
            if line.startswith('MSG: '):
                typename = line[5:]
                continue
                
            # Handle array notation including fixed-size arrays like float64[36]
            m = re.match(r'^([\w/]+)(\[(\d*)\])?\s+(\w+)(?:\s*=.*)?', line)
            if m:
                t, _, array_size, name = m.groups()
                is_array = bool(array_size is not None)
                # For fixed-size arrays, append size to type
                if array_size and array_size.isdigit():
                    t = f"{t}[{array_size}]"
                elif is_array:
                    t = f"{t}[]"
                typedef.append((t, name, is_array))
                
        all_defs[typename or '__main__'] = typedef
    return all_defs

class BufferUnderrunError(Exception):
    """Raised when trying to read beyond buffer bounds"""
    pass

def safe_read(buf: BytesIO, size: int, context: str = "") -> bytes:
    """Safely read from buffer with validation."""
    remaining = buf.getbuffer().nbytes - buf.tell()
    if size > remaining:
        raise BufferUnderrunError(
            f"Tried to read {size} bytes but only {remaining} available. Context: {context}"
        )
    
    data = buf.read(size)
    if len(data) != size:
        raise BufferUnderrunError(
            f"Expected {size} bytes but got {len(data)}. Context: {context}"
        )
    
    return data

def safe_unpack(fmt: str, buf: BytesIO, context: str = "") -> tuple:
    """Safely unpack from buffer with validation."""
    size = struct.calcsize(fmt)
    try:
        data = safe_read(buf, size, context)
        return struct.unpack(fmt, data)
    except BufferUnderrunError:
        raise
    except struct.error as e:
        raise BufferUnderrunError(f"Struct unpack failed: {e}. Context: {context}")

def _deserialize_field(field_type: str, buf: BytesIO, all_defs: Dict, depth: int = 0, context: str = "") -> Any:
    """Deserialize a single field from the buffer with improved error handling."""
    
    # Prevent infinite recursion
    if depth > 100:
        log.error(f"Maximum recursion depth exceeded for field type: {field_type} in {context}")
        return f"[max_depth_exceeded: {field_type}]"
    
    try:
        if field_type == 'string':
            try:
                length, = safe_unpack('<I', buf, f"string length for {field_type} in {context}")
                
                # Sanity check for string length
                if length > 10 * 1024 * 1024:  # 10MB limit
                    log.warning(f"Unusually large string length: {length} in {context}")
                    return buf.read(length)  # Return raw binary data
                
                if length == 0:
                    return ""
                
                string_data = safe_read(buf, length, f"string data ({length} bytes) in {context}")
                return string_data.decode('utf-8', errors='replace')
            except BufferUnderrunError as e:
                #log.warning(f"Buffer underrun reading string in {context}: {e}")
                return buf.read()  # Return remaining raw binary data
        
        elif field_type in ('time', 'duration'):
            try:
                secs, nsecs = safe_unpack('<II', buf, f"{field_type} fields in {context}")
                if field_type == 'time':
                    return ROSTime(secs, nsecs)
                return {'secs': secs, 'nsecs': nsecs}
            except BufferUnderrunError as e:
                #log.warning(f"Buffer underrun reading {field_type} in {context}: {e}")
                return buf.read()  # Return remaining raw binary data
        
        elif field_type in _TYPE_MAP:
            fmt, size = _TYPE_MAP[field_type]
            try:
                result, = safe_unpack('<' + fmt, buf, f"primitive type {field_type} in {context}")
                return result
            except BufferUnderrunError as e:
                #log.warning(f"Buffer underrun reading {field_type} in {context}: {e}")
                return buf.read(size)  # Return raw binary data for the expected size
        
        # Handle fixed-size arrays like float64[36]
        elif '[' in field_type and field_type.count('[') == 1:
            base_type, array_part = field_type.split('[', 1)
            array_size = array_part.rstrip(']')
            
            if array_size.isdigit():  # Fixed-size array
                count = int(array_size)
                return _deserialize_fixed_array(base_type, count, buf, all_defs, depth, context)
            else:  # Variable-length array
                return _deserialize_variable_array(base_type, buf, all_defs, depth, context)
        
        # Handle variable-length arrays (legacy format)
        elif field_type.endswith('[]'):
            base_type = field_type[:-2]
            return _deserialize_variable_array(base_type, buf, all_defs, depth, context)
        
        elif field_type in all_defs:
            return _deserialize_msg(buf, all_defs[field_type], all_defs, depth + 1, context)
        
        elif '/' in field_type:
            # Try without namespace prefix
            short_type = field_type.split('/')[-1]
            if short_type in all_defs:
                return _deserialize_msg(buf, all_defs[short_type], all_defs, depth + 1, context)
        
        log.warning(f"Unsupported field type: {field_type} in {context}")
        return buf.read()  # Return raw binary data for unsupported field types
    
    except BufferUnderrunError as e:
        log.warning(f"Buffer underrun reading {field_type} in {context}: {e}")
        return buf.read()  # Return remaining raw binary data
    except Exception as e:
        log.error(f"Error deserializing field {field_type} in {context}: {e}")
        return buf.read()  # Return remaining raw binary data

def _deserialize_fixed_array(base_type: str, count: int, buf: BytesIO, all_defs: Dict, depth: int, context: str) -> List[Any]:
    """Deserialize fixed-size array with error handling."""
    array_context = f"{context}.{base_type}[{count}]"
    try:
        if base_type in _TYPE_MAP:
            fmt, size = _TYPE_MAP[base_type]
            total_size = size * count
            
            # Check if we have enough data
            remaining = buf.getbuffer().nbytes - buf.tell()
            if total_size > remaining:
                log.warning(f"Not enough data for fixed array in {array_context}: need {total_size}, have {remaining}")
                # Return partial array with available data
                available_count = remaining // size
                if available_count > 0:
                    data = safe_read(buf, available_count * size, f"partial fixed array in {array_context}")
                    partial_result = list(struct.unpack('<' + fmt * available_count, data))
                    # Pad with zeros for missing elements
                    partial_result.extend([0] * (count - available_count))
                    return partial_result
                else:
                    return [0] * count  # Return array of zeros
            
            data = safe_read(buf, total_size, f"fixed array in {array_context}")
            return list(struct.unpack('<' + fmt * count, data))
        else:
            # Complex type array
            result = []
            for i in range(count):
                try:
                    element = _deserialize_field(base_type, buf, all_defs, depth + 1, f"{array_context}[{i}]")
                    result.append(element)
                except BufferUnderrunError:
                    log.warning(f"Buffer underrun at element {i}/{count} of fixed array in {array_context}")
                    break
            return result
            
    except BufferUnderrunError as e:
        log.warning(f"Buffer underrun in fixed array {array_context}: {e}")
        return [f"[incomplete_array: {base_type}]"]

def _deserialize_variable_array(base_type: str, buf: BytesIO, all_defs: Dict, depth: int, context: str) -> List[Any]:
    """Deserialize variable-length array with error handling."""
    array_context = f"{context}.{base_type}[]"
    try:
        count, = safe_unpack('<I', buf, f"array length for {array_context}")
        
        # Sanity check for array length
        if count > 10_000_000:  # 10M elements limit
            log.warning(f"Unusually large array length: {count} in {array_context}")
            return [f"[large_array: {count} elements]"]
        
        if count == 0:
            return []
        
        if base_type in _TYPE_MAP:
            fmt, size = _TYPE_MAP[base_type]
            total_size = size * count
            
            # Check if we have enough data
            remaining = buf.getbuffer().nbytes - buf.tell()
            if total_size > remaining:
                log.warning(f"Not enough data for variable array in {array_context}: need {total_size}, have {remaining}")
                # Return partial array
                available_count = remaining // size
                if available_count > 0:
                    data = safe_read(buf, available_count * size, f"partial variable array in {array_context}")
                    return list(struct.unpack('<' + fmt * available_count, data))
                else:
                    return []
            
            data = safe_read(buf, total_size, f"variable array in {array_context}")
            return list(struct.unpack('<' + fmt * count, data))
        else:
            # Complex type array
            result = []
            for i in range(count):
                try:
                    element = _deserialize_field(base_type, buf, all_defs, depth + 1, f"{array_context}[{i}]")
                    result.append(element)
                except BufferUnderrunError:
                    log.warning(f"Buffer underrun at element {i}/{count} of variable array in {array_context}")
                    break
            return result
            
    except BufferUnderrunError as e:
        log.warning(f"Buffer underrun reading variable array length for {array_context}: {e}")
        return []

def _deserialize_msg(buf: BytesIO, fields: List[Tuple], all_defs: Dict, depth: int = 0, context: str = "") -> Dict:
    """Deserialize a complete message from the buffer with error handling."""
    out = {}
    
    # Track buffer position for debugging
    initial_pos = buf.tell()
    total_size = buf.getbuffer().nbytes
    
    log.debug(f"Deserializing message in {context} at pos {initial_pos}/{total_size}, {len(fields)} fields, depth={depth}")
    
    for field_idx, (field_type, field_name, is_array) in enumerate(fields):
        current_pos = buf.tell()
        remaining = total_size - current_pos
        field_context = f"{context}.{field_name}" if context else field_name
        
        log.debug(f"Field {field_idx}: {field_name} ({field_type}) in {context}, pos={current_pos}, remaining={remaining}")
        
        try:
            if is_array and not ('[' in field_type and field_type.endswith(']')):
                # Variable-length array (old format)
                base_type = field_type.replace('[]', '') if field_type.endswith('[]') else field_type
                out[field_name] = _deserialize_variable_array(base_type, buf, all_defs, depth + 1, field_context)
            else:
                out[field_name] = _deserialize_field(field_type, buf, all_defs, depth + 1, field_context)
                
        except BufferUnderrunError as e:
            log.warning(f"Buffer underrun on field '{field_name}' ({field_type}) in {context}: {e}")
            out[field_name] = f"[incomplete: {field_name}]"
            break  # Stop processing remaining fields
        except Exception as e:
            log.error(f"Error on field '{field_name}' ({field_type}) in {context}: {e}")
            out[field_name] = f"[error: {field_name}]"
            continue  # Try to continue with remaining fields
    
    final_pos = buf.tell()
    log.debug(f"Message deserialization complete for {context}: {initial_pos} -> {final_pos} ({final_pos - initial_pos} bytes)")
    
    return out


def parse_header_block(data: bytes) -> Dict[str, bytes]:
    """Parse header block into key-value pairs."""
    header = {}
    offset = 0
    while offset < len(data):
        if offset + 4 > len(data):
            break
        flen = struct.unpack('<L', data[offset:offset+4])[0]
        offset += 4
        if offset + flen > len(data):
            break
        field = data[offset:offset+flen]
        offset += flen
        if b'=' in field:
            key, val = field.split(b'=', 1)
            header[key.decode('utf-8', errors='replace')] = val
    return header

class StandaloneRosbagParser:
    """Standalone ROS bag parser with no ROS dependencies."""
    
    def __init__(self, filename: str, mode: str = 'r'):
        if mode != 'r':
            raise ValueError("Only read mode is supported")
        
        self.filename = filename
        self.file = open(filename, 'rb')
        self.connections: Dict[int, Dict] = {}
        self.chunks: List[Tuple] = []
        self._read_preamble()
        self._scan_file()

    def _read_preamble(self):
        """Read and validate bag file header."""
        self.file.seek(0)
        preamble = self.file.read(13)
        log.debug(f"Preamble: {preamble}")
        if not preamble.startswith(b'#ROSBAG V2.0'):
            raise ValueError(f"Invalid bag file: {self.filename}")

    def _scan_file(self):
        """Scan file for connections and chunks."""
        self.file.seek(13)
        while True:
            record = self._read_record()
            if not record:
                break
            
            header, data = record
            op = header.get('op', b'\xff')[0]
            log.debug(f"Scan op={op:02x}, header keys={list(header.keys())}")
            
            if op == OP_CONNECTION:
                conn_id = int.from_bytes(header['conn'], 'little')
                topic = header['topic'].decode('utf-8', errors='replace')
                inner = parse_header_block(data)
                self.connections[conn_id] = {
                    'topic': topic, 
                    'header': header, 
                    'inner': inner
                }
                log.debug(f"Found connection {conn_id}: {topic}")
                
            elif op == OP_CHUNK:
                self.chunks.append((header, data))
                log.debug(f"Found chunk with {len(data)} bytes")

    def _read_record(self) -> Optional[Tuple[Dict, bytes]]:
        """Read a single record from the bag file."""
        head = self.file.read(4)
        if not head:
            return None
        
        hlen = struct.unpack('<L', head)[0]
        if hlen == 0:
            return None
            
        hraw = self.file.read(hlen)
        if len(hraw) != hlen:
            return None
            
        header = self._parse_header_fields(hraw)
        
        dlen_raw = self.file.read(4)
        if len(dlen_raw) != 4:
            return None
            
        dlen = struct.unpack('<L', dlen_raw)[0]
        data = self.file.read(dlen)
        if len(data) != dlen:
            return None
            
        return header, data

    def _parse_header_fields(self, raw: bytes) -> Dict[str, bytes]:
        """Parse header fields from raw bytes."""
        return parse_header_block(raw)

    def get_topics(self) -> List[str]:
        """Get list of all topics in the bag."""
        return [conn['topic'] for conn in self.connections.values()]

    def get_message_count(self, topics: Optional[List[str]] = None) -> int:
        """Get total number of messages (optionally filtered by topics)."""
        count = 0
        for _, _, _ in self.read_messages(topics=topics):
            count += 1
        return count

    def get_connection_info(self) -> Dict[str, Dict]:
        """Get information about all connections."""
        info = {}
        for conn_id, conn in self.connections.items():
            topic = conn['topic']
            msg_type = conn['inner'].get('type', b'').decode('utf-8', errors='replace')
            info[topic] = {
                'connection_id': conn_id,
                'message_type': msg_type,
                'md5sum': conn['inner'].get('md5sum', b'').decode('utf-8', errors='replace'),
                'message_definition': conn['inner'].get('message_definition', b'').decode('utf-8', errors='replace')
            }
        return info

    def read_messages(self, topics: Optional[List[str]] = None, exclude_topics: Optional[List[str]] = None):
        """
        Read messages from the bag file.
        
        Args:
            topics: List of topics to filter by, or None for all topics
            
        Yields:
            Tuple of (topic, timestamp, message_data)
        """
        def expand_class(cls):
            return [(t, n, t.endswith('[]') or '[' in t) for t, n in zip(cls._slot_types, cls.__slots__)]

        def register_recursive(msg_type, cls, out):
            if msg_type in out:
                return
            typedef = expand_class(cls)
            out[msg_type] = typedef
            for t in cls._slot_types:
                base = t.replace('[]', '')
                if base in _ros_type_map:
                    register_recursive(base, _ros_type_map[base], out)

        processed_messages = 0
        log.debug(f"Starting to read messages from {len(self.chunks)} chunks")
        log.debug(f"Available connections: {list(self.connections.keys())}")
        
        for chunk_idx, (chunk_header, chunk_data) in enumerate(self.chunks):
            compression = chunk_header.get('compression', b'none').decode('utf-8', errors='replace')
            log.debug(f"Processing chunk {chunk_idx+1}/{len(self.chunks)} with compression: {compression}, size: {len(chunk_data)}")
            
            try:
                if compression == 'none':
                    data = chunk_data
                elif compression == 'bz2':
                    data = bz2.decompress(chunk_data)
                elif compression == 'lz4':
                    data = lz4.frame.decompress(chunk_data)
                else:
                    log.warning(f"Unsupported compression: {compression}")
                    continue
            except Exception as e:
                log.error(f"Failed to decompress chunk: {e}")
                continue

            log.debug(f"Decompressed chunk size: {len(data)}")
            offset = 0
            chunk_messages = 0
            
            while offset < len(data):
                try:
                    if offset + 4 > len(data):
                        log.debug(f"Not enough data for header length at offset {offset}")
                        break
                        
                    hlen = struct.unpack('<L', data[offset:offset+4])[0]
                    offset += 4
                    
                    if hlen == 0 or offset + hlen > len(data):
                        log.debug(f"Invalid header length {hlen} at offset {offset}")
                        break
                        
                    hraw = data[offset:offset+hlen]
                    offset += hlen
                    header = self._parse_header_fields(hraw)

                    if offset + 4 > len(data):
                        log.debug(f"Not enough data for data length at offset {offset}")
                        break
                        
                    dlen = struct.unpack('<L', data[offset:offset+4])[0]
                    offset += 4
                    
                    if offset + dlen > len(data):
                        log.debug(f"Not enough data for message data at offset {offset}, need {dlen}")
                        break
                        
                    d = data[offset:offset+dlen]
                    offset += dlen

                    op = header.get('op', b'\xff')
                    if not op or op[0] != OP_MSG:
                        log.debug(f"Skipping non-message record, op={op[0] if op else 'None'}")
                        continue

                    if 'conn' not in header:
                        log.debug("No connection ID in header")
                        continue
                        
                    conn_id = int.from_bytes(header['conn'], 'little')
                    secs, nsecs = struct.unpack('<II', header['time'])
                    timestamp = secs + nsecs * 1e-9
                    conn = self.connections.get(conn_id)

                    if not conn:
                        log.debug(f"Unknown connection ID: {conn_id}")
                        continue

                    topic = conn['topic']
                    if topics and topic not in topics:
                        continue

                    if exclude_topics and topic in exclude_topics:
                        continue

                    msg_type = conn['inner'].get('type', b'').decode('utf-8', errors='replace').strip()
                    msg_def = conn['inner'].get('message_definition', b'').decode('utf-8', errors='replace')

                    log.debug(f"Processing message: topic={topic}, type={msg_type}, data_len={len(d)}")
                    
                    # Try built-in type registry first
                    if msg_type in _ros_type_map:
                        log.debug(f"Using built-in class for {msg_type}")
                        cls = _ros_type_map[msg_type]
                        all_defs = {}
                        register_recursive(msg_type, cls, all_defs)
                        typedef = all_defs[msg_type]
                    else:
                        # Parse message definition as fallback
                        all_defs = parse_all_msgs(msg_def) if msg_def.strip() else {}
                        log.debug(f"Message definition keys: {list(all_defs.keys())}")
                        
                        if not all_defs:
                            log.warning(f"Unknown message type: {msg_type} and no message definition")
                            continue
                            
                        # Find appropriate typedef
                        if msg_type in all_defs:
                            typedef = all_defs[msg_type]
                        elif msg_type.split('/')[-1] in all_defs:
                            typedef = all_defs[msg_type.split('/')[-1]]
                        elif '__main__' in all_defs and all_defs['__main__']:
                            typedef = all_defs['__main__']
                        else:
                            # Find first non-empty typedef
                            valid_typedefs = [td for td in all_defs.values() if td]
                            if valid_typedefs:
                                typedef = valid_typedefs[0]
                            else:
                                log.warning(f"No valid typedef found for {msg_type}")
                                continue

                    log.debug(f"Using typedef: {typedef}")
                    # Deserialize message
                    #parsed = _deserialize_msg(BytesIO(d), typedef, all_defs)
                    context_str = f"{topic}[{chunk_idx}]"
                    parsed = _deserialize_msg(BytesIO(d), typedef, all_defs, context=context_str)

                    processed_messages += 1
                    chunk_messages += 1
                    
                    yield topic, parsed, timestamp

                except Exception as e:
                    log.error(f"Error processing message at offset {offset}: {e}")
                    import traceback
                    log.error(traceback.format_exc())
                    break
            
            log.debug(f"Chunk {chunk_idx+1} processed {chunk_messages} messages")

        log.debug(f"Total messages processed: {processed_messages}")

    def close(self):
        """Close the bag file."""
        if self.file and not self.file.closed:
            self.file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __repr__(self):
        return f"StandaloneRosbagParser('{self.filename}', {len(self.connections)} connections, {len(self.chunks)} chunks)"


# Backwards compatibility
Bag = StandaloneRosbagParser

# Example usage and debugging
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python bag12.py <bagfile>")
        sys.exit(1)
    
    bag_file = sys.argv[1]
    
    # Set logging to INFO to see debug messages
    logging.getLogger().setLevel(logging.INFO)
    
    try:
        with StandaloneRosbagParser(bag_file) as bag:
            print(f"Bag info: {bag}")
            print(f"Topics: {bag.get_topics()}")
            
            # Print connection info
            print("\nConnection Info:")
            for topic, info in bag.get_connection_info().items():
                print(f"  {topic}: {info['message_type']}")
            
            # Try to read first few messages
            print(f"\nTrying to read messages...")
            message_count = 0
            for topic, timestamp, msg in bag.read_messages():
                message_count += 1
                print(f"Message {message_count}: {topic} @ {timestamp}")
                print(f"  Data keys: {list(msg.keys()) if isinstance(msg, dict) else type(msg)}")
                if message_count >= 3:  # Show first 3 messages
                    break
            
            if message_count == 0:
                print("No messages found! Check the debug logs above.")
            else:
                print(f"\nSuccessfully read {message_count} messages")
                
    except Exception as e:
        print(f"Error opening bag file: {e}")
        import traceback
        traceback.print_exc()