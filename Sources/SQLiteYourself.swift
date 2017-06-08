
import Foundation
import SQLite3

public enum Column {
    case text(String)
    case integer(Int)
    case real(Double)
    case date(Date)
    case data(Data)
}

public enum ColumnType: Int32 {
    case integer = 1 // SQLITE_INTEGER
    case float = 2 // SQLITE_FLOAT
    case text = 3 // SQLITE_TEXT
    case blob = 4 // SQLITE_BLOB
    case null = 5 // SQLITE_NULL
    case date = 42 // NOTE(vdka): This isn't an actual ColumnType defined in SQLite. It's adhoc
}

func getError(_ handle: OpaquePointer) -> String {
    return String(cString: sqlite3_errmsg(handle))
}

/// Internal DateFormatter instance used to manage date formatting
let fmt = DateFormatter()

public class DB {

    public struct Error: Swift.Error {
        var description: String
    }

    static func open(path: String?) throws -> DB {

        var db: OpaquePointer?

        let res = sqlite3_open_v2(path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)
        guard res == SQLITE_OK else {
            let message = String(validatingUTF8: sqlite3_errmsg(db))!
            sqlite3_close(db)
            throw Error(description: message)
        }

        return DB(handle: db!)
    }

    public var handle: OpaquePointer

    init(handle: OpaquePointer) {
        self.handle = handle

        if #available(OSX 10.12, *) {
            sqlite3_trace_v2(handle, UInt32(SQLITE_TRACE_STMT | SQLITE_TRACE_ROW | SQLITE_TRACE_CLOSE), { trace, _, p, t -> Int32 in

                switch Int32(trace) {
                case SQLITE_TRACE_STMT:
                    let str = String(validatingUTF8: t!.assumingMemoryBound(to: Int8.self))!
                    print(str)

                case SQLITE_TRACE_ROW:
                    print("Got row for stmt \(p!)")

                case SQLITE_TRACE_CLOSE:
                    break

                case SQLITE_TRACE_PROFILE:
                    break

                default:
                    fatalError()
                }

                return 0
            }, nil)
        }
    }

    public func exec(_ sql: String) throws {

        let err = sqlite3_exec(self.handle, sql, nil, nil, nil)

        guard err == SQLITE_OK else {
            throw Error(description: getError(handle))
        }
    }

    static let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
    static let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

    public func query(_ sql: String, _ params: Any?...) throws -> Rows {

        var stmt: OpaquePointer?
        let res = sqlite3_prepare_v2(handle, sql, -1, &stmt, nil)

        guard res == SQLITE_OK else {
            sqlite3_finalize(stmt)
            throw Error(description: getError(handle))
        }

        if !params.isEmpty {
            let stmtParamCount = sqlite3_bind_parameter_count(stmt)
            guard params.count == numericCast(stmtParamCount) else {
                fatalError("Parameter count mismatch, sql statment was expecting \(stmtParamCount), but you passed \(params.count)")
            }

            var flag: Int32 = 0
            for (index, param) in params.enumerated() {
                let sqlIndex = Int32(index) + 1
                switch param {
                case let param as String:
                    flag = sqlite3_bind_text(stmt, sqlIndex, param, -1, DB.SQLITE_TRANSIENT)

                case let param as Data:
                    flag = sqlite3_bind_blob(stmt, sqlIndex, param.withUnsafeBytes({ UnsafeRawPointer($0) }), numericCast(param.count), DB.SQLITE_TRANSIENT)

                case let param as Date:
                    let dateString = fmt.string(from: param)
                    flag = sqlite3_bind_text(stmt, sqlIndex, dateString, -1, DB.SQLITE_TRANSIENT)

                case let param as Bool:
                    let num: Int32 = param ? 1 : 0
                    flag = sqlite3_bind_int(stmt, sqlIndex, num)

                case let param as Double:
                    flag = sqlite3_bind_double(stmt, sqlIndex, param)

                case let param as Float:
                    let double = Double(param)
                    flag = sqlite3_bind_double(stmt, sqlIndex, double)

                case let param as Int:
                    flag = sqlite3_bind_int64(stmt, sqlIndex, numericCast(param))

                case nil:
                    flag = sqlite3_bind_null(stmt, sqlIndex)

                default:
                    fatalError("Unsupported type in SQL query param \(type(of: param))")
                }

                guard flag == SQLITE_OK else {
                    sqlite3_finalize(stmt)
                    let error = getError(handle)
                    throw Error(description: error)
                }
            }
        }

        return Rows(dbHandle: handle, stmt: stmt!)
    }

    public func queryFirst(_ sql: String) throws -> Rows.Row {
        var rows = try query(sql)
        defer {
            rows.close()
        }
        guard let row = rows.next() else {
            throw Error(description: "No rows!")
        }

        return row
    }
}

public class Rows: IteratorProtocol, Sequence {

    let dbHandle: OpaquePointer
    var stmt: OpaquePointer?

    let columnCount: Int32
    let columnNames: [String]
    let columnTypes: [ColumnType]

    init(dbHandle: OpaquePointer, stmt: OpaquePointer) {
        self.dbHandle = dbHandle
        self.stmt = stmt

        self.columnCount = sqlite3_column_count(stmt)

        var columnNames: [String] = []
        var columnTypes: [ColumnType] = []
        for index in 0..<columnCount {

            let name = String(validatingUTF8: sqlite3_column_name(stmt, index))!
            let type = Rows.getColumnType(stmt: stmt, index: index)
            columnNames.append(name)
            columnTypes.append(type)
        }
        self.columnNames = columnNames
        self.columnTypes = columnTypes
    }

    deinit {
        self.close()
    }

    func close() {
        // The error returned by finalize is indicative of the error returned by the last evaluation of stmt.
        // Hence, we can ignore it.
        _ = sqlite3_finalize(stmt)
        stmt = nil
    }

    public func next() -> Row? {

        guard let stmt = stmt else {
            return nil
        }

        let result = sqlite3_step(stmt)
        guard result == SQLITE_ROW else {
            assert(result != SQLITE_MISUSE, "Please report this error to https://github.com/vdka/SQLiteYourself/issues")

            if result == SQLITE_BUSY {
                // TODO(vdka): Provide a nice way to try this again.
                print("Database engine unable to acquire database locks. For more details read here: https://sqlite.org/rescode.html#busy")
            }

            return nil
        }

        var columns: [Column?] = []
        for index in 0..<columnCount {
            let val = Rows.getColumnValue(stmt: stmt, index: index)
            columns.append(val)
        }

        return Row(columns: columns)
    }

    static func getColumnValue(stmt: OpaquePointer, index: Int32) -> Column? {

        let valueType = Rows.getColumnType(stmt: stmt, index: index)

        switch valueType {
        case .integer:
            let val = sqlite3_column_int64(stmt, index)
            return Column.integer(numericCast(val))

        case .float:
            let val = sqlite3_column_double(stmt, index)
            return Column.real(val)

        case .blob:
            let data = sqlite3_column_blob(stmt, index)
            let size = sqlite3_column_bytes(stmt, index)
            let val = Data(bytes: data!, count: numericCast(size))
            return Column.data(val)

        case .date:
            // Is this a text date
            if let ptr = UnsafeRawPointer(sqlite3_column_text(stmt, index)) {
                let uptr = ptr.bindMemory(to: CChar.self, capacity: 0)
                let txt = String(validatingUTF8: uptr)!
                let set = CharacterSet(charactersIn: "-:")
                if txt.rangeOfCharacter(from:set) != nil {
                    // Convert to time
                    var time: tm = tm(tm_sec: 0, tm_min: 0, tm_hour: 0, tm_mday: 0, tm_mon: 0, tm_year: 0, tm_wday: 0, tm_yday: 0, tm_isdst: 0, tm_gmtoff: 0, tm_zone:nil)
                    strptime(txt, "%Y-%m-%d %H:%M:%S", &time)
                    time.tm_isdst = -1
                    let diff = TimeZone.current.secondsFromGMT()
                    let t = mktime(&time) + diff
                    let ti = TimeInterval(t)
                    let val = Date(timeIntervalSince1970: ti)
                    return Column.date(val)
                }
            }
            // If not a text date, then it's a time interval
            let timestamp = sqlite3_column_double(stmt, index)
            let val = Date(timeIntervalSince1970: timestamp)
            return Column.date(val)

        case .null:
            return nil

        case .text:

            // If nothing works, return a string representation
            guard let ptr = UnsafeRawPointer(sqlite3_column_text(stmt, index)) else {
                return nil
            }
            let uptr = ptr.bindMemory(to: CChar.self, capacity: 0)
            // FIXME(vdka): Use Swift 4's new Unicode interface
            let val = String(validatingUTF8: uptr)
            return Column.text(val!)
        }
    }

    static func getColumnType(stmt: OpaquePointer, index: Int32) -> ColumnType {

        // Column types - http://www.sqlite.org/datatype3.html (section 2.2 table column 1)
        let blobTypes = ["BINARY", "BLOB", "VARBINARY"]
        let charTypes = ["CHAR", "CHARACTER", "CLOB", "NATIONAL VARYING CHARACTER", "NATIVE CHARACTER", "NCHAR", "NVARCHAR", "TEXT", "VARCHAR", "VARIANT", "VARYING CHARACTER"]
        let intTypes  = ["BIGINT", "BIT", "BOOL", "BOOLEAN", "INT", "INT2", "INT8", "INTEGER", "MEDIUMINT", "SMALLINT", "TINYINT"]
        let nullTypes = ["NULL"]
        let realTypes = ["DECIMAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT", "NUMERIC", "REAL"]
        let dateTypes = ["DATE", "DATETIME", "TIME", "TIMESTAMP"]

        // Determine type of column - http://www.sqlite.org/c3ref/c_blob.html
        guard let cDeclType = sqlite3_column_decltype(stmt, index) else {
            // Expressions and sub-queries do not have `decltype` set
            return ColumnType(rawValue: sqlite3_column_type(stmt, index))!
        }
        let declType = String(validatingUTF8: cDeclType)!.uppercased()

        if intTypes.contains(declType) {
            return ColumnType.integer
        }
        if dateTypes.contains(declType) {
            return ColumnType.date
        }
        if realTypes.contains(declType) {
            return ColumnType.float
        }
        if charTypes.contains(declType) {
            return ColumnType.text
        }
        if blobTypes.contains(declType) {
            return ColumnType.blob
        }
        if nullTypes.contains(declType) {
            return ColumnType.null
        }

        // default everything to text
        return ColumnType.text
    }

    public class Row {
        var columns: [Column?]
        var scanIndex: Int = 0
        // FIXME(vdka): Investigate some sort of threadlocal storage for this then, raise it out off of Row to avoid alloc's
        var data = UnsafeMutableRawBufferPointer.allocate(count: 1024) // start with 1kb

        init(columns: [Column?]) {
            self.columns = columns
        }

        deinit {
            data.deallocate()
        }

        /// Resets the current scanIndex back to 0
        func reset() {
            scanIndex = 0
        }

        func get<T: SQLDataType>(index: Int) -> T? {

            return columns[index].map(T.getFromColumn)
        }

        func scan<T>(_ type: T.Type) -> T {
            assert(scanIndex < columns.count)

            // FIXME: handle optionals possibly with an additional overload
            switch type {
            case is Int.Type:
                guard case .integer(let v)? = columns[scanIndex] else {
                    fatalError("Column type mismatch, expected \(type), got \(String(describing: columns[scanIndex]))")
                }
                scanIndex += 1
                return v as! T

            case is String.Type:
                guard case .text(let v)? = columns[scanIndex] else {
                    fatalError("Column type mismatch, expected \(type), got \(String(describing: columns[scanIndex]))")
                }
                scanIndex += 1
                return v as! T

            case is Double.Type:
                guard case .real(let v)? = columns[scanIndex] else {
                    fatalError("Column type mismatch, expected \(type), got \(String(describing: columns[scanIndex]))")
                }
                scanIndex += 1
                return v as! T

            case is Float.Type:
                guard case .real(let v)? = columns[scanIndex] else {
                    fatalError("Column type mismatch, expected \(type), got \(String(describing: columns[scanIndex]))")
                }
                scanIndex += 1
                return Float(v) as! T

            case is Date.Type:
                guard case .date(let v)? = columns[scanIndex] else {
                    fatalError("Column type mismatch, expected \(type), got \(String(describing: columns[scanIndex]))")
                }
                scanIndex += 1
                return v as! T

            case is Data.Type:
                guard case .data(let v)? = columns[scanIndex] else {
                    fatalError("Column type mismatch, expected \(type), got \(String(describing: columns[scanIndex]))")
                }
                scanIndex += 1
                return v as! T

            default:
                break
            }

            // Create a dummy instance of a value with type `T`
            // Using our dummy data, create an instance of a value with type `T`. This value is never actually used aside to get its Type into the Mirror
            // It is important not to use the value from this memory

            // Firstly ensure we have enough memory, grow the data buffer if we do not.
            if data.count < MemoryLayout<T>.size {
                data.deallocate()
                data = UnsafeMutableRawBufferPointer.allocate(count: MemoryLayout<T>.size)
            }

            _ = data.initializeMemory(as: UInt8.self, from: repeatElement(0, count: MemoryLayout<T>.size))

            let mirror = Mirror(reflecting: data.baseAddress!.bindMemory(to: type, capacity: 1).pointee)
            // NOTE(vdka): @IMPORTANT 
            // DO NOT print anything from the mirror, the if any of the child types are a string then it will be an invalid one.
            // This is because the string represented by a zero'd value will fail in `initializeWithCopy value witness for Swift.String`.
//            print(Array(mirror.children.map({ $0.value }))) // DO NOT DO THIS

            switch mirror.displayStyle! {
            case .tuple: fallthrough
            case .struct:

                let p = UnsafeMutableRawPointer(data.baseAddress!)
                var byteOffset = 0
                for child in mirror.children {
//                    print(byteOffset, terminator: "")

                    defer {
                        scanIndex += 1
                    }

                    // we need to pack for alignment in tuples and structs
                    defer {
                        let rem = byteOffset % 8
                        if rem != 0 {
                            byteOffset += rem
//                            print(" -> \(byteOffset)")
                        } else {
//                            print()
                        }
                    }

                    switch child.value {
                    case let value as Int:
                        guard case .integer(let v)? = columns[scanIndex] else {
                            fatalError("Column type mismatch, expected \(type), got \(String(describing: columns[scanIndex]))")
                        }
                        p.storeBytes(of: v, toByteOffset: byteOffset, as: Int.self)
                        byteOffset += MemoryLayout.alignment(ofValue: value)

                    case let value as String:
                        guard case .text(let v)? = columns[scanIndex] else {
                            fatalError("Column type mismatch, expected \(type), got \(String(describing: columns[scanIndex]))")
                        }
                        p.storeBytes(of: v, toByteOffset: byteOffset, as: String.self)
                        byteOffset += MemoryLayout.alignment(ofValue: value)

                    case let value as Double:
                        guard case .real(let v)? = columns[scanIndex] else {
                            fatalError("Column type mismatch, expected \(type), got \(String(describing: columns[scanIndex]))")
                        }
                        p.storeBytes(of: v, toByteOffset: byteOffset, as: Double.self)
                        byteOffset += MemoryLayout.alignment(ofValue: value)

                    case let value as Float:
                        guard case .real(let v)? = columns[scanIndex] else {
                            fatalError("Column type mismatch, expected \(type), got \(String(describing: columns[scanIndex]))")
                        }
                        p.storeBytes(of: Float(v), toByteOffset: byteOffset, as: Float.self)
                        byteOffset += MemoryLayout.stride(ofValue: value)

                    case let value as Date:
                        guard case .date(let v)? = columns[scanIndex] else {
                            fatalError("Column type mismatch, expected \(type), got \(String(describing: columns[scanIndex]))")
                        }
                        p.storeBytes(of: v, toByteOffset: byteOffset, as: Date.self)
                        byteOffset += MemoryLayout.alignment(ofValue: value)

                    case let value as Data:
                        guard case .data(let v)? = columns[scanIndex] else {
                            fatalError("Column type mismatch, expected \(type), got \(String(describing: columns[scanIndex]))")
                        }
                        p.storeBytes(of: v, toByteOffset: byteOffset, as: Data.self)
                        byteOffset += MemoryLayout.stride(ofValue: value)

                    default:
                        fatalError("Unsupported type")
                    }
                }

            case .optional:
                fatalError("Soon™")

            default:
                fatalError("Unsupported kind for reflection. Only unnested value types are supported by .scan")
            }

            return data.baseAddress!.assumingMemoryBound(to: type).pointee
        }
    }
}

protocol SQLDataType {
    static func getFromColumn(_ column: Column) -> Self
}
extension Int: SQLDataType {
    static func getFromColumn(_ column: Column) -> Int {
        guard case .integer(let v) = column else {
            fatalError()
        }
        return v
    }
}
extension String: SQLDataType {
    static func getFromColumn(_ column: Column) -> String {
        guard case .text(let v) = column else {
            fatalError()
        }
        return v
    }
}

