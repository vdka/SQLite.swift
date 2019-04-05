
import Foundation
import SQLite3

// MARK: Types

let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

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

public struct SQLiteOpenFlags: OptionSet {
    public let rawValue: Int32
    public init(rawValue: Int32) { self.rawValue = rawValue }

    public static let readOnly = SQLiteOpenFlags(rawValue: SQLITE_OPEN_READONLY)
    public static let readWrite = SQLiteOpenFlags(rawValue: SQLITE_OPEN_READWRITE)
    public static let create = SQLiteOpenFlags(rawValue: SQLITE_OPEN_CREATE)
    public static let openUri = SQLiteOpenFlags(rawValue: SQLITE_OPEN_URI)
    public static let openMemory = SQLiteOpenFlags(rawValue: SQLITE_OPEN_MEMORY)
    public static let noMutex = SQLiteOpenFlags(rawValue: SQLITE_OPEN_NOMUTEX)
    public static let fullMutex = SQLiteOpenFlags(rawValue: SQLITE_OPEN_FULLMUTEX)
    public static let sharedCache = SQLiteOpenFlags(rawValue: SQLITE_OPEN_SHAREDCACHE)
    public static let privateCache = SQLiteOpenFlags(rawValue: SQLITE_OPEN_PRIVATECACHE)
}

public class Database {
    typealias Handle = OpaquePointer

    public let filepath: String
    let handle: Handle
    let queue = DispatchQueue(label: "me.vdka.Transact.SQLite3DB.Handle", qos: .userInteractive, attributes: [])

    var hasOpenRows = false

    public static var defaultOpenFlags: SQLiteOpenFlags = [.readWrite, .create, .sharedCache]

    public init(filepath: String, flags: SQLiteOpenFlags = Database.defaultOpenFlags) throws {
        var dbHandle: Handle?
        let result = sqlite3_open_v2(filepath, &dbHandle, flags.rawValue, nil)
        guard let handle = dbHandle, result == SQLITE_OK else {
            let error = Error.new(dbHandle)
            sqlite3_close(dbHandle)
            throw error
        }
        self.filepath = filepath
        self.handle = handle
    }

    deinit {
        let result = queue.sync { [handle] in
            sqlite3_close_v2(handle)
        }
        _ = result
    }

    public class Rows {
        var stmt: StatementHandle
        var db: Database

        public var columnCount: Int32 = 0
        public var error: Database.Error?
        public lazy var columnNames: [String] = {
            if error != nil { return [] }
            return (0..<columnCount)
                .map({ sqlite3_column_name(stmt, $0) })
                .map({ String(cString: $0) })
        }()

        init(stmt: StatementHandle, db: Database) {
            self.stmt = stmt
            self.db = db

            self.columnCount = sqlite3_column_count(stmt)
            db.hasOpenRows = true
        }

        deinit {
            db.queue.sync {
                if error == nil && sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_RUN, 0) == 0 {
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
                        print("WARNING: Rows deallocated before ever calling next on them")
                    }
                }
                sqlite3_finalize(stmt)
                db.hasOpenRows = false
            }
        }
    }

    public class Row {
        var stmt: StatementHandle
        var db: Database

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

    class PreparedStatement {
        var handle: StatementHandle
        unowned var db: Database

        var error: Error?

        lazy var isReadOnly: Bool = sqlite3_stmt_readonly(handle) != 0

        init(handle: StatementHandle, db: Database, error: Error? = nil) {
            self.handle = handle
            self.db = db
            self.error = error
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
            let stmt = prepare(sql, args: args)
            return Rows(stmt: stmt.handle, db: self)
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

    func exec(_ sql: String, args: SQLDataType?...) -> Row {
        return exec(sql, args: args)
    }

    func exec(_ sql: String, args: [SQLDataType?]) -> Row {
        let row = queryRow(sql, args: args)
        if row.error?.code == SQLITE_DONE {
            row.error = nil
        }
        return row
    }
}

extension Database {

    func prepare(_ sql: String, args: [SQLDataType?]) -> PreparedStatement {
        var stmtHandle: StatementHandle?
        let result = sql.utf8CString.withUnsafeBufferPointer { buffer in
            return sqlite3_prepare_v2(handle, buffer.baseAddress, numericCast(sql.count), &stmtHandle, nil)
        }
        guard let stmt = stmtHandle, result == SQLITE_OK else {
            let error = Error.new(handle)
            return PreparedStatement(handle: undef(), db: self, error: error)
        }
        assert(sqlite3_bind_parameter_count(stmt) == args.count)
        for (index, arg) in args.map({ $0?.sqlColumnValue }).enumerated() {
            let sqlIndex = Int32(index + 1) // The leftmost value in SQLite has an index of 1
            var flag: Int32 = 0
            switch arg {
            case .text(let param)?:
                flag = sqlite3_bind_text(stmt, sqlIndex, param, Int32(param.utf8.count), SQLITE_TRANSIENT)
            case .blob(let param)?:
                flag = param.withUnsafeBytes {
                    return sqlite3_bind_blob(stmt, sqlIndex, UnsafeRawPointer($0), Int32(param.count), SQLITE_TRANSIENT)
                }
            case .real(let param)?:
                flag = sqlite3_bind_double(stmt, sqlIndex, param)
            case .integer(let param)?:
                flag = sqlite3_bind_int64(stmt, sqlIndex, numericCast(param))
            case nil:
                flag = sqlite3_bind_null(stmt, sqlIndex)
            }
            if flag != SQLITE_OK {
                let error = Error.new(handle)
                return PreparedStatement(handle: undef(), db: self, error: error)
            }
        }
        return PreparedStatement(handle: stmt, db: self)
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
        defer { columnIndex += 1 }
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
        guard error == nil, columnIndex < rows.columnCount else { return Optional<Any?>.none }
        let value = scanAny()
        return .some(value)
    }
}

extension Database: Hashable {

    public static func == (lhs: Database, rhs: Database) -> Bool {
        return lhs.handle == rhs.handle
    }

    public var hashValue: Int {
        return queue.hashValue
    }
}
