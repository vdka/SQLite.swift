
import SQLite3
import Foundation

public class Pool {
    public let filepath: String

    var traceOptions: Database.TraceOptions = []
    var timeout: Int?

    let queue = DispatchQueue(label: "me.vdka.SQLiteYourself.Pool", qos: .userInteractive)

    let writer: Database

    var drainQueued = false
    var drainDelay: TimeInterval = 1.0

    public var readyReaders: Set<Database>
    public var takenReaders: Set<Database> = []

    public static let writerFlags: SQLiteOpenFlags = [.readWrite, .sharedCache, .create]
    public static let readerFlags: SQLiteOpenFlags = [.readOnly,  .sharedCache]

    public init(filepath: String) throws {
        switch filepath {
        case ":memory":
            fallthrough
        case "":
            let msg = "Database pooling cannot be used with path \(filepath)"
            throw Database.Error(code: SQLITE_ERROR, description: msg)
        default: break
        }
        self.filepath = filepath
        self.writer = try Database(filepath: filepath, flags: Pool.writerFlags)
        writer.queryRow("PRAGMA journal_mode=WAL")

        let firstReader = try Database(filepath: filepath, flags: Pool.readerFlags)
        self.readyReaders = [firstReader]
    }

    func execute<T>(closure: (Database) -> T) throws -> T {
        let db = try queue.sync { () -> Database in
            if !readyReaders.isEmpty {
                print("Using existing DB")
                return readyReaders.removeFirst()
            } else {
                print("Spawning new DB")
                let db = try Database(filepath: filepath, flags: Pool.readerFlags)
                if !traceOptions.isEmpty {
                    db.enableTrace(options: traceOptions)
                }
                if let timeout = timeout {
                    db.setTimeout(timeout)
                }
                return db
            }
        }
        takenReaders.insert(db)
        let value = closure(db)

        if db.hasOpenRows {
            queue.sync {
                takenReaders.remove(db)
                readyReaders.insert(db)
            }
        }
        return value
    }

    func queueDrain() {
        if drainQueued { return }
        drainQueued = true
        queue.asyncAfter(deadline: .now() + drainDelay) { [weak self] in
            guard let self = self else { return }
            self.drain()
        }
    }

    func drain() {
        drainQueued = false
        if readyReaders.count <= 1 { return }
        if !takenReaders.isEmpty {
            queueDrain()
            return
        }

        let noLongerInUse = takenReaders.filter({ !$0.hasOpenRows })
        takenReaders = takenReaders.subtracting(noLongerInUse)
        readyReaders = readyReaders.union(noLongerInUse)

        let db = readyReaders.removeFirst()
        readyReaders = [db]
    }
}

extension Pool {

    @discardableResult
    func exec(_ sql: String, args: SQLDataType?...) -> Database.Row {
        return writer.exec(sql, args: args)
    }

    @discardableResult
    func exec(_ sql: String, args: [SQLDataType?]) -> Database.Row {
        return writer.exec(sql, args: args)
    }

    @discardableResult
    func query(_ sql: String, args: SQLDataType?...) -> Database.Rows {
        return query(sql, args: args)
    }

    @discardableResult
    func query(_ sql: String, args: [SQLDataType?]) -> Database.Rows {
        do {
            return try execute { db in
                let stmt = db.prepare(sql, args: args)
                if !stmt.isReadOnly {
                    // Trigger deinit so finalize is called
                    sqlite3_finalize(stmt.handle)
                    print("WARNING: Write transaction done in query. Use exec instead")
                    let row = exec(sql, args: args)
                    return row.rows
                }
                return Database.Rows(stmt: stmt.handle, db: db)
            }
        } catch let error as Database.Error {
            let rows = Database.Rows(stmt: undef(), db: undef())
            rows.error = error
            return rows
        } catch let unknownError {
            print("An unknown error did occur \(unknownError)")
            let error = Database.Error(code: SQLITE_ERROR, description: "An unknown error occured")
            let rows = Database.Rows(stmt: undef(), db: undef())
            rows.error = error
            return rows
        }
    }

    @discardableResult
    func queryRow(_ sql: String, args: SQLDataType?...) -> Database.Row {
        return queryRow(sql, args: args)
    }

    @discardableResult
    func queryRow(_ sql: String, args: [SQLDataType?]) -> Database.Row {
        let rows = query(sql, args: args)
        guard let row = rows.next() else {
            let row = Database.Row(stmt: rows.stmt, db: rows.db, rows: rows)
            row.error = Database.Error(code: SQLITE_DONE, description: "No rows")
            return row
        }
        return row
    }
}

public extension Pool {

    func enableTrace(options: Database.TraceOptions = [.traceProfile]) {
        self.traceOptions = options
        assert(takenReaders.isEmpty, "\(#function) must be called while no other operations are in progress")
        queue.sync {
            writer.enableTrace(options: traceOptions)
            readyReaders.forEach {
                $0.enableTrace(options: traceOptions)
            }
        }
    }

    func disableTrace() {
        self.traceOptions = []
        assert(takenReaders.isEmpty, "\(#function) must be called while no other operations are in progress")
        queue.sync {
            writer.disableTrace()
            readyReaders.forEach {
                $0.disableTrace()
            }
        }
    }

    func setTimeout(_ timeout: Int) {
        self.timeout = timeout
        assert(takenReaders.isEmpty, "\(#function) must be called while no other operations are in progress")
        queue.sync {
            writer.setTimeout(timeout)
            readyReaders.forEach {
                $0.setTimeout(timeout)
            }
        }
    }
}
