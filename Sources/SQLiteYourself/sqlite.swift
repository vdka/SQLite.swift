
import Foundation
import SQLite3

// MARK: Types

private let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

typealias StatementHandle = OpaquePointer

public enum Column {
    case text(String)
    case integer(Int)
    case real(Double)
    case blob(Data)
}

enum ColumnType: Int32 {
    case integer = 1 // SQLITE_INTEGER
    case real = 2 // SQLITE_FLOAT
    case text = 3 // SQLITE_TEXT
    case blob = 4 // SQLITE_BLOB
    case null = 5 // SQLITE_NULL
}

public class Database {

    public typealias Handle = OpaquePointer

    public let filepath: String
    let handle: Handle
    let queue = DispatchQueue(label: "me.vdka.Transact.SQLite3DB.Handle", qos: .userInteractive, attributes: [])

    public init(filepath: String) throws {
        var dbHandle: Handle?
        let result = sqlite3_open_v2(filepath, &dbHandle, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)
        guard let handle = dbHandle, result == SQLITE_OK else {
            let error = Error.new(dbHandle)
            sqlite3_close(dbHandle)
            throw error
        }
        self.filepath = filepath
        self.handle = handle
    }

    deinit {
        queue.sync { [handle] in
            sqlite3_close_v2(handle)
        }
    }

    public class Rows {
        var stmt: StatementHandle
        unowned var db: Database

        public var columnCount: Int32 = 0
        public var error: Database.Error?
        public lazy var columnNames: [String] = {
            if error != nil { return [] }
            return (0..<columnCount)
                .map({ sqlite3_column_name(stmt, $0) })
                .map({ String(cString: $0) })
        }()

        init(stmt: StatementHandle, db: Database) {
            assert(String(cString: __dispatch_queue_get_label(nil), encoding: .utf8) == db.queue.label)

            self.stmt = stmt
            self.db = db

            self.columnCount = sqlite3_column_count(stmt)
        }

        deinit {
            db.queue.sync {
                if sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_RUN, 0) == 0 {
                    if sqlite3_stmt_readonly(stmt) == 0 {
                        // Check if the query hasn't been run
                        // This statement will have side effects, for those, we step once. To apply them.
                        // The statement is deallocating before being run, let's run it once first.

                        var result = sqlite3_step(stmt)
                        repeat {
                            switch result {
                            case SQLITE_ROW:
                                break
                            case SQLITE_BUSY:
                                result = sqlite3_step(stmt)
                                continue
                            case SQLITE_DONE:
                                break
                            default:
                                // Some error ...
                                // TODO: What do we do here?
                                return
                            }
                        } while result == SQLITE_BUSY
                    } else {
                        print("Rows deallocated before ever calling next on them")
                    }
                }
                sqlite3_finalize(stmt)
            }
        }
    }

    public class Row {
        var stmt: StatementHandle
        unowned var db: Database

        public var rows: Rows
        public var columnIndex: Int32 = 0
        public var error: Error? {
            didSet { rows.error = error }
        }

        var types: [ColumnType] = []

        init(stmt: StatementHandle, db: Database, rows: Rows) {
            self.stmt = stmt
            self.db = db
            self.rows = rows
            self.error = rows.error

            for index in 0..<rows.columnCount {
                let datatype = sqlite3_column_type(stmt, index)
                let type = ColumnType(rawValue: datatype) ?? .null
                types.append(type)
            }
        }
    }

    public struct Error: Swift.Error, CustomStringConvertible {
        public var code: Int32
        public var description: String

        static func new(_ handle: OpaquePointer?) -> Error {
            let code = sqlite3_errcode(handle)
            let msg = String(cString: sqlite3_errmsg(handle))

            return Error(code: code, description: msg)
        }
    }
}

public extension Database {

    @discardableResult
    func query(_ sql: String, args: SQLDataType?...) -> Rows {
        return query(sql, args: args)
    }

    @discardableResult
    func query(_ sql: String, args: [SQLDataType?]) -> Rows {
        return queue.sync {
            var stmtHandle: StatementHandle?

            let result = sql.utf8CString.withUnsafeBufferPointer { buffer in
                return sqlite3_prepare_v2(handle, buffer.baseAddress, numericCast(sql.count), &stmtHandle, nil)
            }

            guard let stmt = stmtHandle, result == SQLITE_OK else {
                let rows = Rows(stmt: undef(), db: self)
                rows.error = Error.new(handle)
                return rows
            }

            assert(sqlite3_bind_parameter_count(stmt) == args.count)

            let rows = Rows(stmt: stmt, db: self)
            for (index, arg) in args.map({ $0?.sqlColumnValue }).enumerated() {
                let sqlIndex = Int32(index + 1) // The leftmost value in SQLite has an index of 1
                var flag: Int32 = 0
                switch arg {
                case .text(let param)?:
                    flag = sqlite3_bind_text(stmt, sqlIndex, param, Int32(param.utf8.count), SQLITE_TRANSIENT)

                case .blob(let param)?:
                    param.withUnsafeBytes {
                        flag = sqlite3_bind_blob(stmt, sqlIndex, UnsafeRawPointer($0), Int32(param.count), SQLITE_TRANSIENT)
                    }

                case .real(let param)?:
                    flag = sqlite3_bind_double(stmt, sqlIndex, param)

                case .integer(let param)?:
                    flag = sqlite3_bind_int64(stmt, sqlIndex, numericCast(param))

                case nil:
                    flag = sqlite3_bind_null(stmt, sqlIndex)
                }

                if flag != SQLITE_OK {
                    rows.error = Error.new(handle)
                }
            }

            return rows
        }
    }

    @discardableResult
    func queryRow(_ sql: String, args: SQLDataType?...) -> Row {
        return queryRow(sql, args: args)
    }

    @discardableResult
    func queryRow(_ sql: String, args: [SQLDataType?]) -> Row {
        let rows = query(sql, args: args)
        guard let row = rows.next() else {
            let row = Row(stmt: rows.stmt, db: self, rows: rows)
            row.error = Error(code: SQLITE_DONE, description: "No rows")
            return row
        }
        return row
    }
}

public func undef() -> String {
    return ""
}

public func undef<T>() -> Optional<T> {
    return .none
}

/// undef is used internally within SQLiteYourself to provide an undefined return value
public func undef<T>() -> T {
    let pointer = UnsafeMutablePointer<T>.allocate(capacity: 1)
    pointer.withMemoryRebound(to: UInt8.self, capacity: MemoryLayout<T>.size, {
        for offs in (0..<MemoryLayout<T>.size) {
            $0.advanced(by: offs).pointee = 0
        }
    })
    defer { pointer.deallocate() }
    return pointer.pointee
}

// row.scan(&a).scan(&b)
// row.scan() | row.scan(Type.self)
// row.scan(default: undef()) | row.scan(Type.self, default: undef())
// row.scan() | row.scan(Type?.self)
// row.scanAny()

extension Database.Row {

    public func scanAny() -> Any? {
        if self.error != nil { return nil }
        switch scanColumn() {
        case .integer(let val)?:
            return val

        case .real(let val)?:
            return val

        case .blob(let val)?:
            return val

        case .text(let val)?:
            return val

        case nil:
            return nil
        }
    }

    @discardableResult
    public func scan<Type: SQLDataType>(_ value: inout Type) -> Database.Row {
        if self.error != nil { return self }
        value = scan(Type.self)
        return self
    }

    public func scan<Type: SQLDataType>(_ type: Type.Type, default: Type = undef()) -> Type {
        if self.error != nil { return undef() }
        guard let value = scan(Type?.self) else {
            self.error = Database.Error.init(
                code: 0, description: "Scanned NULL from database where value was expected")
            return undef()
        }
        return value
    }

    public func scan<Type: SQLDataType>(_ type: Optional<Type>.Type) -> Type? {
        if self.error != nil { return nil }
        return scanColumn().map(Type.get)
    }

    private func scanColumn() -> Column? {
        guard columnIndex < rows.columnCount else {
            // Set error?
            return nil
        }
        return db.queue.sync {
            switch types[Int(columnIndex)] {
            case .integer:
                let val = sqlite3_column_int64(stmt, columnIndex)
                return Column.integer(Int(val))

            case .real:
                let val = sqlite3_column_double(stmt, columnIndex)
                return Column.real(val)

            case .blob:
                let ptr = sqlite3_column_blob(stmt, columnIndex)
                let size = sqlite3_column_bytes(stmt, columnIndex)
                guard let data = ptr, size != 0 else {
                    return Column.blob(Data())
                }
                let val = Data(bytes: data, count: Int(size))
                return Column.blob(val)

            case .text:
                guard let ptr = UnsafeRawPointer(sqlite3_column_text(stmt, columnIndex)) else {
                    return nil
                }

                let val = String(cString: ptr.assumingMemoryBound(to: UInt8.self))
                return Column.text(val)

            case .null:
                return nil
            }
        }
    }
}

extension Database.Rows: IteratorProtocol, Sequence {

    public func next() -> Database.Row? {
        guard error == nil else { return nil }
        return db.queue.sync {
            var result = sqlite3_step(stmt)
            repeat {
                switch result {
                case SQLITE_ROW: // More rows available
                    break
                case SQLITE_DONE: // No more rows available
                    return nil
                case SQLITE_BUSY:
                    result = sqlite3_step(stmt)
                    continue
                default:
                    return nil
                }
            } while result == SQLITE_BUSY
            return Database.Row(stmt: stmt, db: db, rows: self)
        }
    }
}

extension Database.Row: IteratorProtocol, Sequence {

    // Doubly wrapped any ... boy oh boy.
    public func next() -> Any?? {
        guard error == nil, columnIndex < rows.columnCount else { return .none }
        let value = db.queue.sync {
            return self.scanAny()
        }
        return .some(value)
    }
}
