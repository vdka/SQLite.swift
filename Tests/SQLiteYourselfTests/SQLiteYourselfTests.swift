import XCTest
@testable import SQLiteYourself

struct User: Equatable {

    let firstName: String
    let lastName: String
    let age: Float
    let email: String

    static func == (lhs: User, rhs: User) -> Bool {
        return lhs.firstName == rhs.firstName &&
            lhs.lastName == rhs.lastName &&
            lhs.age == rhs.age &&
            lhs.email == rhs.email
    }
}

struct UserWithId: Equatable {
    var id: Int
    let firstName: String
    let lastName: String
    let age: Float
    let email: String

    static func == (lhs: UserWithId, rhs: UserWithId) -> Bool {
        return lhs.id == rhs.id
    }
}

class SQLiteYourselfTests: XCTestCase {

    var databasePath: String = ""

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

        db.queryRow("CREATE TABLE users (dob INTEGER NOT NULL)")

        let now = Date()
        db.queryRow("INSERT INTO users VALUES (?)", args: now)

        let harryDob = db.queryRow("SELECT dob FROM users").scan(Date.self)
        XCTAssertEqual(now.timeIntervalSince1970, harryDob.timeIntervalSince1970)
    }

    func testStringStorage() {
        let db = try! Database(filepath: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])

        db.setTimeout(500)

        db.queryRow("CREATE TABLE users (name TEXT NOT NULL)")
        db.queryRow("INSERT INTO users VALUES (?)", args: "Harry")

        let harryName = db.queryRow("SELECT name FROM users").scan(String.self)
        XCTAssertEqual(harryName, "Harry")
    }

    func testBoolStorage() {
        let db = try! Database(filepath: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])

        db.setTimeout(500)

        db.queryRow("CREATE TABLE users (wizard BOOL NOT NULL)")
        db.queryRow("INSERT INTO users VALUES (?)", args: true)
        db.queryRow("INSERT INTO users VALUES (?)", args: false)

        let isWiz = db.queryRow("SELECT wizard FROM users WHERE wizard").scan(Bool.self)
        XCTAssert(isWiz)
        let noWiz = db.queryRow("SELECT wizard FROM users WHERE NOT wizard").scan(Bool.self)
        XCTAssertFalse(noWiz)
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
