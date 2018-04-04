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

        let db = try! DB.open(path: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])

        db.setTimeout(500)

        try! db.exec("CREATE TABLE users (dob INTEGER NOT NULL)")

        let now = Date()
        try! db.exec("INSERT INTO users VALUES (?)", params: now)

        let harryDob = try! db.queryFirst("SELECT dob FROM users")!.scan(Date.self)
        XCTAssertEqual(now.timeIntervalSince1970, harryDob.timeIntervalSince1970)
    }

    func testStringStorage() {
        let db = try! DB.open(path: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])

        db.setTimeout(500)

        try! db.exec("CREATE TABLE users (name TEXT NOT NULL)")
        try! db.exec("INSERT INTO users VALUES (?)", params: "Harry")

        let harryName = try! db.queryFirst("SELECT name FROM users")!.scan(String.self)
        XCTAssertEqual(harryName, "Harry")
    }

    func testBoolStorage() {
        let db = try! DB.open(path: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])

        db.setTimeout(500)

        try! db.exec("CREATE TABLE users (wizard BOOL NOT NULL)")
        try! db.exec("INSERT INTO users VALUES (?)", params: true)

        let harryYoung = try! db.queryFirst("SELECT wizard FROM users")!.scan(Bool.self)
        XCTAssert(harryYoung)
    }

    func testFloatStorage() {
        let db = try! DB.open(path: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])

        db.setTimeout(500)

        try! db.exec("CREATE TABLE users (height FLOAT NOT NULL)")
        try! db.exec("INSERT INTO users VALUES (?)", params: 168.7)

        let harryHeight = try! db.queryFirst("SELECT height FROM users")!.scan(Float.self)
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
