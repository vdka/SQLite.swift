
import SQLite3

extension Database {
    public func setTimeout(_ timeout: Int) {
        sqlite3_busy_timeout(handle, numericCast(timeout))
    }
}
