import '../errors/errors.dart';
import '../primitives/buffer.dart';
import '../utils/indent.dart';
import 'codecs.dart';

class SQLRecordColumn {
  final String name;
  final Codec codec;

  SQLRecordColumn(this.name, this.codec);
}

class SQLRecordCodec extends Codec {
  final List<SQLRecordColumn> columns;
  final ReturnTypeConstructor? returnType;

  SQLRecordCodec(super.tid, List<Codec> codecs, List<String> names,
      {this.returnType})
      : columns = List.generate(codecs.length, (i) {
          return SQLRecordColumn(names[i], codecs[i]);
        });

  @override
  void encode(WriteBuffer buf, dynamic object) {
    throw ArgumentError("SQL records cannot be passed as arguments");
  }

  @override
  dynamic decode(ReadBuffer buf) {
    final els = buf.readUint32();
    if (els != columns.length) {
      throw ProtocolError(
          'cannot decode SQL record: expected ${columns.length} elements, got $els');
    }

    final result = <String, dynamic>{};
    for (var col in columns) {
      buf.discard(4); // reserved
      final elemLen = buf.readInt32();
      final name = col.name;
      if (elemLen == -1) {
        result[name] = null;
      } else {
        final elemBuf = buf.slice(elemLen);
        result[name] = col.codec.decode(elemBuf);
        elemBuf.finish();
      }
    }

    return returnType != null ? returnType!(result) : result;
  }

  @override
  String toString() {
    return 'SQLRecordCodec ($tid) {\n'
        '${columns.map((field) => '  ${field.name}:'
            ' ${indent(field.codec.toString())}\n').join('')}'
        '}';
  }

  @override
  bool compare(Codec codec) {
    if (codec is! SQLRecordCodec || codec.columns.length != columns.length) {
      return false;
    }
    for (var i = 0; i < columns.length; i++) {
      if (columns[i].name != codec.columns[i].name ||
          !columns[i].codec.compare(codec.columns[i].codec)) {
        return false;
      }
    }
    return true;
  }
}
