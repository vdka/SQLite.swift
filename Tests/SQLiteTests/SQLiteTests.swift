import XCTest
import SQLite3
@testable import SQLite

class SQLiteTests: XCTestCase {

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

    func testReadme() {
        struct User: Equatable {
            let firstName: String?
            let lastName: String
            let age: Float
            let email: String

            init(firstName: String?, lastName: String, age: Float, email: String) {
                self.firstName = firstName
                self.lastName = lastName
                self.age = age
                self.email = email
            }

            init(row: Database.Row) {
                self.firstName = row.scan()
                self.lastName = row.scan()
                self.age = row.scan()
                self.email = row.scan()
            }
        }

        let db = try! Pool(filepath: databasePath)

        let error = db.exec(
            """
            CREATE TABLE users (
                id INTEGER,
                first_name TEXT,
                last_name TEXT NOT NULL,
                age FLOAT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                PRIMARY KEY(id)
            );
            """
        ).error
        XCTAssertNil(error)

        let u1 = User(firstName: "Thurstan", lastName: "Bussy", age: 34, email: "tbussy0@w3.org")
        let u2 = User(firstName: "Zoe", lastName: "Shufflebotham", age: 66, email: "zshufflebotham1@accuweather.com")
        let u3 = User(firstName: nil, lastName: "McKinstry", age: 33, email: "cmckinstry2@state.gov")
        let u4 = User(firstName: "Valma", lastName: "Mulvin", age: 31, email: "vmulvin3@ustream.tv")

        let users: [User] = [u1, u2, u3, u4]

        for user in users {
            let error = db.exec("INSERT INTO users (first_name, last_name, age, email) VALUES (?, ?, ?, ?)",
                    args: user.firstName, user.lastName, user.age, user.email).error
            XCTAssertNil(error)
        }

        let rows = db.query("SELECT first_name, last_name, age, email FROM users ORDER BY age ASC")
        for (row, expected) in zip(rows, users.sorted(by: { $0.age < $1.age })) {
            let user = User(row: row)
            XCTAssertNil(row.error)
            XCTAssertEqual(user, expected)
        }

        let row = db.queryRow("SELECT first_name, last_name, age, email FROM users WHERE email LIKE '%@%.gov'")
        let govEmployee = User(row: row)
        XCTAssertNil(row.error)
        XCTAssertEqual(govEmployee, u3)
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
