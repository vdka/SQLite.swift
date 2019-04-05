import XCTest
import SQLite3
@testable import SQLiteYourself

class SQLiteYourselfTests: XCTestCase {

    var databasePath: String = ""

    override class func setUp() {
        super.setUp()
        print("SQLite Version " + SQLITE_VERSION)
    }

    override func setUp() {
        super.setUp()
        databasePath = (Bundle(for: type(of: self)).bundlePath as NSString).appendingPathComponent("db.sqlite")
        if FileManager.default.fileExists(atPath: databasePath) {
            try! FileManager.default.removeItem(atPath: databasePath)
        }
    }

    func testDateStorage() {
        let db = try! Database(filepath: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])
        db.setTimeout(500)

        let createRow = db.exec("CREATE TABLE users (dob INTEGER NOT NULL)")
        XCTAssertNil(createRow.error)

        let now = Date()
        db.queryRow("INSERT INTO users VALUES (?)", args: now)

        var harryDob = Date()
        let row = db.queryRow("SELECT dob FROM users").scan(&harryDob)
        XCTAssertNil(row.error)
        XCTAssertEqual(now.timeIntervalSince1970, harryDob.timeIntervalSince1970)
    }

    func testStringStorage() {
        let db = try! Database(filepath: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])
        db.setTimeout(500)

        db.queryRow("CREATE TABLE users (name TEXT NOT NULL)")
        db.queryRow("INSERT INTO users VALUES (?)", args: "Harry")

        let row = db.queryRow("SELECT name FROM users")
        XCTAssertNil(row.error)
        let harryName = row.scanAny() as? String
        XCTAssertEqual(harryName, "Harry")
    }

    func testBoolStorage() {
        let db = try! Pool(filepath: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])
        db.setTimeout(500)

        var error: Database.Error?

        error = db.exec("CREATE TABLE users (wizard BOOL NOT NULL)").error
        XCTAssertNil(error)

        error = db.exec("INSERT INTO users VALUES (?)", args: true).error
        XCTAssertNil(error)

        error = db.exec("INSERT INTO users VALUES (?)", args: false).error
        XCTAssertNil(error)

        var wiz = false

        error = db.queryRow("SELECT wizard FROM users WHERE wizard").scan(&wiz).error
        XCTAssertNil(error)
        XCTAssert(wiz)
        error = db.queryRow("SELECT wizard FROM users WHERE NOT wizard").scan(&wiz).error
        XCTAssertNil(error)
        XCTAssertFalse(wiz)
    }

    func testFloatStorage() {
        let db = try! Database(filepath: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])
        db.setTimeout(500)

        db.queryRow("CREATE TABLE users (height FLOAT NOT NULL)")
        db.queryRow("INSERT INTO users VALUES (?)", args: 168.7)

        let harryHeight = db.queryRow("SELECT height FROM users").scan(Float.self)
        XCTAssertEqual(harryHeight, 168.7)
    }

    func testIntStorage() {
        let db = try! Database(filepath: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])
        db.setTimeout(500)

        db.queryRow("CREATE TABLE users (height INTEGER NULL)")
        db.queryRow("INSERT INTO users VALUES (?)", args: 168)
        db.queryRow("INSERT INTO users VALUES (?)", args: 188)

        let rows = db.query("SELECT * FROM users WHERE height > ?", args: 180)
        XCTAssertNil(rows.error)

        let row = rows.next()
        XCTAssertNotNil(row)

        let height = row!.scan(Int.self, default: 0)
        XCTAssertNil(row!.error)
        XCTAssertEqual(height, 188)

    }

    static var allTests: [(String, () -> Void)] = [
    ]
}

func dumpMemory<T>(of input: T, nBytes: Int = MemoryLayout<T>.size) {

    var input = input

    withUnsafeBytes(of: &input) { buffer in

        for offset in buffer.indices {
            if offset % 8 == 0 && offset != 0 { print("\n", terminator: "") }

            let byte = buffer.load(fromByteOffset: offset, as: UInt8.self)
            let hexByte = String(byte, radix: 16)

            // Pad the output to be 2 characters wide
            if hexByte.count == 1 { print("0", terminator: "") }
            print(hexByte, terminator: " ")
        }
        print("")
    }
}
