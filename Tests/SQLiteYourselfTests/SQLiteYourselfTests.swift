import XCTest
@testable import SQLiteYourself

class SQLiteYourselfTests: XCTestCase {

    var databasePath: String = ""

    override func setUp() {
        super.setUp()

        databasePath = (Bundle(for: type(of: self)).bundlePath as NSString).appendingPathComponent("db.sqlite")
        guard FileManager.default.fileExists(atPath: databasePath) else {
            return
        }
        try! FileManager.default.removeItem(atPath: databasePath)
    }

    func testExample() {

        let db = try! DB.open(path: databasePath)

        try! db.exec("""
                CREATE TABLE users (
                    id INTEGER,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    age INTEGER NOT NULL,
                    email TEXT NOT NULL UNIQUE,
                    PRIMARY KEY(id)
                );
            """)

        try! db.exec("INSERT INTO users VALUES (1, 'Gary', 'Doe', 23, 'gary@gmail.com')")

        for _ in 0 ..< 1 {
            let rows = try! db.query("SELECT id, age, email FROM users WHERE (id = ?)", 1)
            guard let row = rows.next() else {
                XCTFail("Failed to find Gary!")
                return
            }

            let (id, age, email) = row.scan((Int, Int, String).self)

            XCTAssertEqual(id, 1)
            XCTAssertEqual(age, 23)
            XCTAssertEqual(email, "gary@gmail.com")

            row.reset()

            XCTAssertEqual(row.scan(Int.self), 1)
            XCTAssertEqual(row.scan(Int.self), 23)
            XCTAssertEqual(row.scan(String.self), "gary@gmail.com")

            row.reset()

            struct User {
                let id: Int
                let age: Int
                let email: String
            }

            let user = row.scan(User.self)
            XCTAssertEqual(user.id, 1)
            XCTAssertEqual(user.age, 23)
            XCTAssertEqual(user.email, "gary@gmail.com")
        }
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
