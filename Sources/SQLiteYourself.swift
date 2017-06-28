
import Foundation
import SQLite3

public enum ColumnType: Int32 {
    case integer = 1 // SQLITE_INTEGER
    case real = 2 // SQLITE_FLOAT
    case text = 3 // SQLITE_TEXT
    case blob = 4 // SQLITE_BLOB
    case null = 5 // SQLITE_NULL
}


public protocol DBInterface {
    var handle: DB.Handle { get }
    var queue: DispatchQueue { get }
    func exec(_ sql: StaticString, params: SQLDataType?...) throws
    func query(_ sql: StaticString, params: SQLDataType?...) throws -> Rows
    func queryFirst(_ sql: StaticString, params: SQLDataType?...) throws -> Rows.Row?
}

public class Tx: DBInterface {

    public let handle: DB.Handle
    public let queue: DispatchQueue

    init(handle: DB.Handle, queue: DispatchQueue) {
        self.handle = handle
        self.queue = queue
    }

    public func commit() throws {
        try exec("COMMIT")
    }

    public func rollback() throws {
        try exec("ROLLBACK")
    }

    public enum Mode {

        /// Deferred means that no locks are acquired on the database until the database is first accessed.
        /// Thus with a deferred transaction, the BEGIN statement itself does nothing to the filesystem.
        /// Locks are not acquired until the first read or write operation.
        /// The first read operation against a database creates a SHARED lock and the first write operation creates a RESERVED lock.
        /// Because the acquisition of locks is deferred until they are needed, it is possible that another thread or process could
        ///  create a separate transaction and write to the database after the BEGIN on the current thread has executed.
        case deferred

        /// If the transaction is immediate, then RESERVED locks are acquired on all databases as soon as the BEGIN command is executed,
        ///  without waiting for the database to be used. After a BEGIN IMMEDIATE, no other database connection will be able to write to
        ///  the database or do a BEGIN IMMEDIATE or BEGIN EXCLUSIVE. Other processes can continue to read from the database, however.
        case immediate

        /// An exclusive transaction causes EXCLUSIVE locks to be acquired on all databases.
        /// After a BEGIN EXCLUSIVE, no other database connection except for read_uncommitted connections will be able to read the
        ///  database and no other connection without exception will be able to write the database until the transaction is complete.
        case exclusive
    }
}

// MARK: - Savepoint stuff. Not sure why these would be used... but eh.
extension Tx {

    public func savepoint() throws -> Savepoint {
        let uuid = UUID()
        try exec("SAVEPOINT ?", params: uuid.uuidString)

        return Savepoint(uuid: uuid)
    }

    public func release(_ savepoint: Savepoint) throws {
        try exec("RELEASE ?", params: savepoint.uuid.uuidString)
    }

    public func rollback(to savepoint: Savepoint) throws {
        try exec("ROLLBACK TO ?", params: savepoint.uuid.uuidString)
    }

    public struct Savepoint {
        let uuid: UUID
    }
}

public class DB: DBInterface {

    public let handle: DB.Handle
    public let queue = DispatchQueue(label: "me.vdka.SQLiteYourself", qos: .userInteractive, attributes: [])

    init(handle: DB.Handle) {
        self.handle = handle
    }

    public static func open(path: String?) throws -> DB {

        var db: DB.Handle?

        let res = sqlite3_open_v2(path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)
        guard res == SQLITE_OK else {
            sqlite3_close(db)
            throw Error.new(db!)
        }

        return DB(handle: db!)
    }

    public func close() {
        queue.sync {
            if #available(OSX 10.10, iOS 8.2, *) {
                sqlite3_close_v2(handle)
            } else {
                sqlite3_close(handle)
            }
        }
    }

    /// Begin starts a transaction
    /// - Parameter mode: The mode to begin the transaction with
    /// - SeeAlso: Tx.Mode
    public func begin(_ mode: Tx.Mode = .deferred) throws -> Tx {

        let tx = Tx(handle: handle, queue: queue)
        switch mode {
        case .deferred:
            try exec("BEGIN") // NOTE: DEFERRED is the default behaviour

        case .exclusive:
            try exec("BEGIN EXCLUSIVE")

        case .immediate:
            try exec("BEGIN IMMEDIATE")
        }

        return tx
    }
}

extension DB {

    public typealias Handle = OpaquePointer
    typealias Stmt = OpaquePointer

    public struct Error: Swift.Error, CustomStringConvertible {
        public var code: Int32
        public var description: String

        static func new(_ handle: Handle) -> Error {
            let code = sqlite3_errcode(handle)
            let msg = String(cString: sqlite3_errmsg(handle))

            return Error(code: code, description: msg)
        }
    }

    static let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
    static let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
}

extension DBInterface {

    /// The number of rows modified, inserted or deleted by the most recently completed INSERT, UPDATE or DELETE statement.
    /// - Note: Changes caused by triggers, foreign key actions or REPLACE constraint resolution are not counted.
    public var rowsAffected: Int {
        return Int(sqlite3_changes(handle))
    }

    /// The rowid of the most recent successful INSERT.
    /// - SeeAlso: https://sqlite.org/c3ref/last_insert_rowid.html
    public var lastInsertId: Int {
        return Int(sqlite3_last_insert_rowid(handle))
    }

    func bind(stmt: DB.Stmt, params: [Column?]) throws {

        if !params.isEmpty {
            let stmtParamCount = sqlite3_bind_parameter_count(stmt)
            guard params.count == numericCast(stmtParamCount) else {
                fatalError("Parameter count mismatch, sql statment was expecting \(stmtParamCount), but you passed \(params.count)")
            }

            var flag: Int32 = 0
            for (index, param) in params.enumerated() {
                let sqlIndex = Int32(index) + 1
                switch param {
                case .text(let param)?:
                    flag = sqlite3_bind_text(stmt, sqlIndex, param, -1, DB.SQLITE_TRANSIENT)

                case .blob(let param)?:
                    flag = sqlite3_bind_blob(stmt, sqlIndex, param.withUnsafeBytes({ UnsafeRawPointer($0) }), numericCast(param.count), DB.SQLITE_TRANSIENT)

                case .real(let param)?:
                    flag = sqlite3_bind_double(stmt, sqlIndex, param)

                case .integer(let param)?:
                    flag = sqlite3_bind_int64(stmt, sqlIndex, numericCast(param))


                case nil:
                    flag = sqlite3_bind_null(stmt, sqlIndex)
                }

                guard flag == SQLITE_OK else {
                    sqlite3_finalize(stmt)
                    throw DB.Error.new(handle)
                }
            }
        }
    }

    /// Exec executes a query without returning any rows
    /// - SeeAlso: lastInsertId
    /// - SeeAlso: rowsAffected
    public func exec(_ sql: StaticString, params: SQLDataType?...) throws {
        try queue.sync {
            var stmt: DB.Stmt?
            var res = sql.withUTF8Buffer { buffer in
                return buffer.baseAddress!.withMemoryRebound(to: CChar.self, capacity: buffer.count) { p in
                    return sqlite3_prepare_v2(handle, p, numericCast(buffer.count), &stmt, nil)
                }
            }

            guard res == SQLITE_OK else {
                throw DB.Error.new(handle)
            }

            try bind(stmt: stmt!, params: params.map({ $0?.sqlColumnValue }))

            res = sqlite3_step(stmt)
            guard res == SQLITE_OK || res == SQLITE_DONE else {
                if res == SQLITE_BUSY {
                    print("DB was busy, this can be tried again!")
                }
                throw DB.Error.new(handle)
            }
        }
    }

    public func query(_ sql: StaticString, params: [SQLDataType?]) throws -> Rows {

        return try queue.sync {
            var stmt: DB.Stmt?

            // This grabs the buffer from the static string `sql` without a copy and needs to jump a couple hoops to get to the expected type.
            let res = sql.withUTF8Buffer { buffer in
                return buffer.baseAddress!.withMemoryRebound(to: CChar.self, capacity: buffer.count) { p in
                    return sqlite3_prepare_v2(handle, p, numericCast(sql.utf8CodeUnitCount), &stmt, nil)
                }
            }

            guard res == SQLITE_OK else {
                sqlite3_finalize(stmt)
                throw DB.Error.new(handle)
            }

            try bind(stmt: stmt!, params: params.map({ $0?.sqlColumnValue }))

            return Rows(stmt: stmt!)
        }
    }

    /// Executes a query that returns rows, typically a SELECT. The args are for any placeholder parameters in the query.
    /// ## Example
    /// ```swift
    ///    let rows = try db.query("SELECT first_name, last_name, age, email FROM users ORDER BY age ASC")
    public func query(_ sql: StaticString, params: SQLDataType?...) throws -> Rows {
        return try query(sql, params: params)
    }

    /// Executes a query that is expected to return at most one row. Errors are deferred until Row's Scan method is called.
    /// ## Example
    /// ```swift
    ///    let employee = try! db.queryFirst("SELECT * FROM users WHERE email LIKE '%@%.gov'")?.scan(User.self)
    public func queryFirst(_ sql: StaticString, params: SQLDataType?...) throws -> Rows.Row? {
        var rows = try query(sql, params: params)
        defer {
            rows.close()
        }
        guard let row = rows.next() else {
            return nil
        }
        
        return row
    }
}

/// Rows is the result of a query. It represents a sequence of individal Rows (`Rows.Row`)
/// - SeeAlso: Rows.Row
/// - Note: Rows retains a reference to the underlying statement allowing it to lazily fetch individual rows from the db as needed.
public class Rows: IteratorProtocol, Sequence {

    var stmt: DB.Stmt?

    public let columnCount: Int32
    public let columnNames: [String]
    public let columnTypes: [ColumnType]

    init(stmt: DB.Stmt) {
        self.stmt = stmt

        self.columnCount = numericCast(sqlite3_column_count(stmt))

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

    /// Close the rows preventing further enumeration. `next()` will return nil after a call to close
    public func close() {
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

        case .real:
            let val = sqlite3_column_double(stmt, index)
            return Column.real(val)

        case .blob:
            let data = sqlite3_column_blob(stmt, index)
            let size = sqlite3_column_bytes(stmt, index)
            let val = Data(bytes: data!, count: numericCast(size))
            return Column.blob(val)

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

    // TODO(vdka): Expose the column named types directly also.
    static func getColumnType(stmt: OpaquePointer, index: Int32) -> ColumnType {

        let datatype = sqlite3_column_type(stmt, index)
        return ColumnType(rawValue: datatype) ?? .null
    }

    /// Row represents a single row of data. Values can be read out using the `scan` functions
    /// - Note: Row is not read lazily
    public class Row {
        public var columns: [Column?]
        var scanIndex: Int = 0

        init(columns: [Column?]) {
            self.columns = columns
        }

        /// Resets the current scanIndex back to 0
        public func reset() {
            scanIndex = 0
        }

        public func get<T: SQLDataType>(index: Int) -> T? {

            return columns[index].map(T.get(from:))
        }

        /// Reads a single non nil value of type `type` from the column at `scanIndex`
        /// - Precondition: `columns[scanIndex] != nil`
        /// - Precondition: `scanIndex < columns.count`
        /// - Note: increments `scanIndex`
        public func scan<T: SQLDataType>(_ type: T.Type) -> T {
            assert(scanIndex < columns.count)
            defer {
                scanIndex += 1
            }

            return T.get(from: columns[scanIndex]!)
        }

        /// Reads a single nullable (Optional) value of type `type` from the column at `scanIndex`
        /// - Precondition: `scanIndex < columns.count`
        /// - Note: increments `scanIndex`
        public func scanOptional<T: SQLDataType>(_ type: T.Type) -> T? {
            assert(scanIndex < columns.count)
            defer {
                scanIndex += 1
            }

            return columns[scanIndex].map(T.get(from:))
        }

        /// Allocates memory for the aggregate (Tuple or Struct) `T` and reads values from the Row in order
        ///   returning an instance of T.
        /// - Precondition: Each member type of the type `T` must conform to `SQLDataType`
        /// - Note: increments `scanIndex` to `columns.count`
        /// ```swift
        ///    db.queryFirst("SELECT name, age, email FROM users")?.scan((String, Int, String).self)
        public func scan<T>(_ aggregateType: T.Type) -> T {

            let metadata = Metadata(type: aggregateType)

            var types: [Any.Type]
            var nullable: [Bool]
            var offsets: [Int]
            switch metadata.kind {
            case .struct:
                let structMetadata = unsafeBitCast(metadata, to: Metadata.Struct.self)
                guard let fieldTypes = structMetadata.fieldTypes else {
                    fatalError("Unable to find subtypes of \(aggregateType)")
                }
                types = fieldTypes
                offsets = structMetadata.fieldOffsets

            case .tuple:
                let tupleMetadata = unsafeBitCast(metadata, to: Metadata.Tuple.self)
                types = tupleMetadata.elementTypes
                offsets = tupleMetadata.elementOffsets

            default:
                fatalError("""
                    \(metadata.kind) is not supported by SQLiteYourself.
                    Only tuple and struct types are currently supported by this method
                    We are working on this.
                    """)
            }

            let storage = UnsafeMutableRawBufferPointer.allocate(count: MemoryLayout<T>.size); defer {
                storage.deallocate()
            }

            for (offset, var ptype) in zip(offsets, types) {

                var allowNull = false

                if extensions(of: ptype).isOptional {
                    allowNull = true
                    let propertyMetadata = Metadata.Enum(type: ptype)!
                    ptype = propertyMetadata.caseTypes!.first!
                }

                guard let propertyType = ptype as? SQLDataType.Type else {

                    fatalError("""

                        ERROR: Unsupported type (\(ptype)) in type \(aggregateType) during call to \(#function)

                        SQLiteYourself only has support for aggregate types that contain type conformant to the SQLDataType protocol.
                        If you beleive your type should conform to this, you can implement the conformance yourself and you will be able to read and write
                        the type to your database directly without error.

                        If you beleive this error should not have occured check GitHub for similar complaints and 👍 them.
                        If no issue exists that describes your use case please create an issue explaining why and how you would use this functionality.

                    """)
                }

                guard let columnValue = columns[scanIndex] else {
                    guard allowNull else {
                        fatalError("Found null value in call to \(#function) where output type was non Optional")
                    }

                    // And now for the tricky part

                    let optionalSize = Metadata(type: ptype).valueWitnessTable.size

                    // An `Optional` is the size of the generic type `Wrapped` + 1 byte.
                    // An `Optional.none` value is all zero's with the last byte being `1`

                    storage.baseAddress!.advanced(by: offset).initializeMemory(as: Int8.self, at: 0, count: optionalSize - 1, to: 0)
                    storage.baseAddress!.advanced(by: offset).advanced(by: optionalSize).assumingMemoryBound(to: Int8.self).initialize(to: 1)
                    scanIndex += 1
                    continue
                }

                let propertyValue = propertyType.get(from: columnValue)

                extensions(of: propertyType).write(propertyValue, to: storage.baseAddress!.advanced(by: offset))
                scanIndex += 1
            }

            return storage.baseAddress!.assumingMemoryBound(to: aggregateType).pointee
        }
    }
}

extension DB {

    public func setTimeout(_ timeout: Int) {
        sqlite3_busy_timeout(handle, numericCast(timeout))
    }
}

// https://sqlite.org/c3ref/c_trace.html
extension DB {

    @available(macOS 10.12, *)
    static func expandStmt(_ stmt: DB.Stmt) -> String {
        if #available(iOS 10.0, *) {
            let expanded = sqlite3_expanded_sql(stmt); defer {
                sqlite3_free(expanded)
            }

            guard let e = expanded, let string = String(validatingUTF8: e) else {
                return "Error expanding SQL Stmt '\(stmt)' for trace"
            }
            return string
        } else {

            let expanded = UnsafeRawPointer(sqlite3_sql(stmt))?.assumingMemoryBound(to: CChar.self)
            guard let e = expanded, let string = String(validatingUTF8: e) else {
                return "Error expanding SQL Stmt '\(stmt)' for trace"
            }
            return string
        }
    }

    static var tracerv1: @convention(c) (UnsafeMutableRawPointer?, UnsafePointer<Int8>?) -> Void = { c, sql in
        guard let c = c, let sql = sql else { return }
        guard let query = String(validatingUTF8: sql) else { return }
        print(query)
    }

    static var profilerv1: @convention(c) (UnsafeMutableRawPointer?, UnsafePointer<Int8>?, UInt64) -> Void = { c, sql, timeNanoseconds in
        guard let c = c, let sql = sql else { return }
        guard let query = String(validatingUTF8: sql) else { return }
        let timeMilliseconds = timeNanoseconds / 10000
        print("\(timeMilliseconds)ms elapsed while running: ")
        print(query)
        print()
    }

    @available(macOS 10.12, *)
    static var tracerv2: @convention(c) (UInt32, UnsafeMutableRawPointer?, UnsafeMutableRawPointer?, UnsafeMutableRawPointer?) -> Int32 = { t, c, p, x in

        ///> The T argument is one of the SQLITE_TRACE constants to indicate why the callback was invoked. The C argument is a copy of the context pointer.
        ///> The P and X arguments are pointers whose meanings depend on T.

        switch Int32(t) {
        case SQLITE_TRACE_STMT:
            /// The P argument is a pointer to the prepared statement.
            /// The X argument is a pointer to a string which is the unexpanded SQL text of the prepared statement or an SQL comment that indicates the
            ///   invocation of a trigger.
            /// The callback can compute the same text that would have been returned by the legacy sqlite3_trace() interface by using the X argument when X
            ///   begins with "--" and invoking sqlite3_expanded_sql(P) otherwise.

            guard
                let x = x?.assumingMemoryBound(to: Int8.self),
                let s = String(validatingUTF8: x),
                s.hasPrefix("--")
                else {
                    let stmt = unsafeBitCast(p!, to: DB.Stmt.self)
                    let query = expandStmt(stmt)
                    print(query)
                    break
            }

            print(s)

        case SQLITE_TRACE_PROFILE:

            guard let timeNanoseconds = x?.assumingMemoryBound(to: Int64.self).pointee else {
                break
            }

            let stmt = unsafeBitCast(p!, to: DB.Stmt.self)
            let query = expandStmt(stmt)
            let timeMilliseconds = timeNanoseconds / 10000
            print("\(timeMilliseconds)ms elapsed while running: ")
            print(query)
            print()

        case SQLITE_TRACE_ROW:

            let stmt = unsafeBitCast(p!, to: DB.Stmt.self)
            let query = expandStmt(stmt)
            print("Row for \(query)")

        case SQLITE_TRACE_CLOSE:
            let handle = unsafeBitCast(p!, to: DB.Handle.self)
            print("DB '\(handle)' closed!")

        default:
            break
        }

        return 0
    }

    public struct TraceOptions: OptionSet {
        public let rawValue: UInt32
        public init(rawValue: UInt32) { self.rawValue = rawValue }
        public static let traceClose   = TraceOptions(rawValue: numericCast(SQLITE_TRACE_CLOSE))
        public static let traceProfile = TraceOptions(rawValue: numericCast(SQLITE_TRACE_PROFILE))
        public static let traceRow     = TraceOptions(rawValue: numericCast(SQLITE_TRACE_ROW))
        public static let traceStmt    = TraceOptions(rawValue: numericCast(SQLITE_TRACE_STMT))
    }

    /// Enable tracing on the database
    /// - SeeAlso: DB.TraceOptions
    public func enableTrace(options: TraceOptions = [.traceProfile]) {

        if #available(OSX 10.12, iOS 10.0, *) {
            sqlite3_trace_v2(handle, options.rawValue, DB.tracerv2, nil)
        } else {
            sqlite3_trace(handle, DB.tracerv1, nil)
            if options.contains(.traceProfile) {
                sqlite3_profile(handle, DB.profilerv1, nil)
            }
        }
    }

    /// Disable tracing on the database
    /// - SeeAlso: DB.TraceOptions
    public func disableTrace() {
        if #available(OSX 10.12, iOS 10.0, *) {
            sqlite3_trace_v2(handle, 0, nil, nil)
        } else {
            sqlite3_trace(handle, nil, nil)
            sqlite3_profile(handle, nil, nil)
        }
    }
}
