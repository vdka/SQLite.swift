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

    func testGeneralUsage() {

        let db = try! DB.open(path: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])

        try! db.exec("""
                CREATE TABLE users (
                    id INTEGER,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    age FLOAT NOT NULL,
                    email TEXT NOT NULL UNIQUE,
                    PRIMARY KEY(id)
                );
            """)

        let u1 = User(firstName: "Thurstan", lastName: "Bussy", age: 34, email: "tbussy0@w3.org")
        let u2 = User(firstName: "Zoe", lastName: "Shufflebotham", age: 66, email: "zshufflebotham1@accuweather.com")
        let u3 = User(firstName: "Constancia", lastName: "McKinstry", age: 33, email: "cmckinstry2@state.gov")
        let u4 = User(firstName: "Valma", lastName: "Mulvin", age: 31, email: "vmulvin3@ustream.tv")

        let users: [User] = [u1, u2, u3, u4]

        let tx = try! db.begin()

        for (index, user) in users.enumerated() {
            try! tx.exec("INSERT INTO users (first_name, last_name, age, email) VALUES (?, ?, ?, ?)", params: user.firstName, user.lastName, user.age, user.email)

            XCTAssertEqual(index + 1, tx.lastInsertId)
            XCTAssertEqual(1, tx.rowsAffected)
        }

        try! tx.commit()

        for _ in 0 ..< 1 {
            let rows = try! db.query("SELECT first_name, last_name, age, email FROM users ORDER BY age ASC")

            for (row, expectedUser) in zip(rows, users.sorted(by: { $0.0.age < $0.1.age })) {
                let user = row.scan(User.self)

                XCTAssertEqual(user, expectedUser)
            }

            let govEmployee = try! db.queryFirst("SELECT first_name, last_name, age, email FROM users WHERE email LIKE '%@%.gov'")!.scan(User.self)
            XCTAssertEqual(govEmployee, u3)

            let namesOf30YearOlds = try! db.query("SELECT first_name, last_name FROM users WHERE age > 30 AND age < 40 ORDER BY age ASC").map({ $0.scan((String, String).self) })
            for (received, expectedUser) in zip(namesOf30YearOlds, [u4, u3, u1]) {
                let expectedTuple = (expectedUser.firstName, expectedUser.lastName)
                XCTAssertEqual(received.0, expectedTuple.0)
                XCTAssertEqual(received.1, expectedTuple.1)
            }
        }

        db.close()
    }

    func testNullableFields() {

        let db = try! DB.open(path: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])

        try! db.exec("""
                CREATE TABLE users (
                    id INTEGER,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    age FLOAT NOT NULL,
                    email TEXT UNIQUE,
                    PRIMARY KEY(id)
                );
            """)



        let u1: (String, String, Float, String?) = ("Thurstan", "Bussy", 34, "tbussy0@w3.org")
        let u2: (String, String, Float, String?) = ("Zoe", "Shufflebotham", 66, nil)
        let u3: (String, String, Float, String?) = ("Constancia", "McKinstry", 33, nil)
        let u4: (String, String, Float, String?) = ("Valma", "Mulvin", 31, "vmulvin3@ustream.tv")

        let users = [u1, u2, u3, u4]

        let tx = try! db.begin()

        for (index, user) in users.enumerated() {
            try! tx.exec("INSERT INTO users (first_name, last_name, age, email) VALUES (?, ?, ?, ?)", params: user.0, user.1, user.2, user.3)

            XCTAssertEqual(index + 1, tx.lastInsertId)
            XCTAssertEqual(1, tx.rowsAffected)
        }

        try! tx.commit()

        let rows = try! db.query("SELECT first_name, last_name, age, email FROM users WHERE email IS NULL")
        for (row, expected) in zip(rows, [u2, u3]) {
            var tuple = row.scan((String, String, Float, String?).self)

            withUnsafeBytes(of: &tuple.3) {
                print(Array($0))
            }
            XCTAssertEqual(tuple.0, expected.0)
            XCTAssertEqual(tuple.1, expected.1)
            XCTAssertEqual(tuple.2, expected.2)
            XCTAssertEqual(tuple.3, expected.3)
        }
    }

    func testAggregateOverscan() {

        let db = try! DB.open(path: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])

        db.setTimeout(500)

        try! db.exec("""
                CREATE TABLE users (
                    id INTEGER,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    age FLOAT NOT NULL,
                    email TEXT UNIQUE,
                    PRIMARY KEY(id)
                );
            """)

        try! db.exec("""
            CREATE TABLE scores (
                id INTEGER,
                user_id INTEGER NOT NULL,
                score INTEGER NOT NULL,
                PRIMARY KEY(id)
            );
            """)


        var u1 = UserWithId(id: 0, firstName: "Thurstan", lastName: "Bussy", age: 34, email: "tbussy0@w3.org")
        var u2 = UserWithId(id: 0, firstName: "Zoe", lastName: "Shufflebotham", age: 66, email: "zshufflebotham1@accuweather.com")

        let tx = try! db.begin()

        try! tx.exec("INSERT INTO users (first_name, last_name, age, email) VALUES (?, ?, ?, ?)", params: u1.firstName, u1.lastName, u1.age, u1.email)
        u1.id = tx.lastInsertId

        try! tx.exec("INSERT INTO users (first_name, last_name, age, email) VALUES (?, ?, ?, ?)", params: u2.firstName, u2.lastName, u2.age, u2.email)
        u2.id = tx.lastInsertId

        try! tx.exec("INSERT INTO scores (user_id, score) VALUES (?, ?)", params: u1.id, 11)
        try! tx.exec("INSERT INTO scores (user_id, score) VALUES (?, ?)", params: u2.id, 7)
        try! tx.exec("INSERT INTO scores (user_id, score) VALUES (?, ?)", params: u1.id, 11)
        try! tx.exec("INSERT INTO scores (user_id, score) VALUES (?, ?)", params: u2.id, 9)

        try! tx.commit()

        let userScorePairs = try! db.query("SELECT users.*, scores.score FROM users INNER JOIN scores ON users.id = scores.user_id WHERE users.first_name = 'Zoe'")
            .map({ ($0.scan(UserWithId.self), $0.scan(Int.self)) })

        XCTAssertEqual(userScorePairs[0].0, u2)
        XCTAssertEqual(userScorePairs[1].0, u2)
        XCTAssertEqual(userScorePairs[0].1, 7)
        XCTAssertEqual(userScorePairs[1].1, 9)
    }

    func testDateStorage() {

        let db = try! DB.open(path: databasePath)
        db.enableTrace(options: [.traceProfile, .traceClose])

        db.setTimeout(500)

        try! db.exec("CREATE TABLE users (name TEXT NOT NULL, dob INTEGER NOT NULL)")

        let now = Date()
        try! db.exec("INSERT INTO users VALUES ('Harry', ?)", params: now)

        let harryDob = try! db.queryFirst("SELECT dob FROM users")!.scan(Date.self)
        XCTAssertEqual(now.timeIntervalSince1970, harryDob.timeIntervalSince1970)
    }

    static var allTests = [
        ("testGeneralUsage", testGeneralUsage),
        ("testNullableFields", testNullableFields),
    ]
}
