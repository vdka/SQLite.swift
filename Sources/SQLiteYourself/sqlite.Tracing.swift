
import SQLite3

// https://sqlite.org/c3ref/c_trace.html
extension Database {

    static func expandStmt(_ stmt: StatementHandle) -> String {
        let expanded = sqlite3_expanded_sql(stmt); defer {
            sqlite3_free(expanded)
        }

        guard let e = expanded, let string = String(validatingUTF8: e) else {
            return "Error expanding SQL Stmt '\(stmt)' for trace"
        }
        return string
    }

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
                    let stmt = unsafeBitCast(p!, to: StatementHandle.self)
                    let query = expandStmt(stmt)
                    print(query)
                    break
            }

            print(s)

        case SQLITE_TRACE_PROFILE:

            guard let timeNanoseconds = x?.assumingMemoryBound(to: Int64.self).pointee else {
                break
            }

            let stmt = unsafeBitCast(p!, to: StatementHandle.self)
            let query = expandStmt(stmt)
            let timeMilliseconds = timeNanoseconds / 10000
            print("\(timeMilliseconds)ms elapsed while running: ")
            print(query)
            print()

        case SQLITE_TRACE_ROW:

            let stmt = unsafeBitCast(p!, to: StatementHandle.self)
            let query = expandStmt(stmt)
            print("Row for \(query)")

        case SQLITE_TRACE_CLOSE:
            let handle = unsafeBitCast(p!, to: Database.Handle.self)
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
        sqlite3_trace_v2(handle, options.rawValue, Database.tracerv2, nil)
    }

    /// Disable tracing on the database
    /// - SeeAlso: DB.TraceOptions
    public func disableTrace() {
        sqlite3_trace_v2(handle, 0, nil, nil)
    }
}


