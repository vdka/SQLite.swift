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

        var rows = try! db.query("SELECT id, age FROM users WHERE (id = 1)")
        guard var row = rows.next() else {
            XCTFail("Failed to find Gary!")
            return
        }

        let details = row.scan((Int, Int).self)
        print(details)

//        XCTAssertEqual(details.1, "gary@gmail.com")
        XCTAssertEqual(details.1, 23)

        row.reset()

        struct User {
            let id: Int
            let age: Int
        }

        let user = row.scan(User.self)
        XCTAssertEqual(user.age, 23)
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
