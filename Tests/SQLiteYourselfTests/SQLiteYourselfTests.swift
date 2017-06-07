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
                    email TEXT NOT NULL UNIQUE,
                    PRIMARY KEY(id)
                );
                """)

        try! db.exec("INSERT INTO users VALUES (1, 'Gary', 'Doe', 'gary@gmail.com')")

        var rows = try! db.query("SELECT * FROM users WHERE (id = 1)")
        guard let row = rows.next() else {
            XCTFail("Failed to find Gary!")
            return
        }

        let email: String = row.get(index: 3)!
        print(email)
        XCTAssertEqual(email, "gary@gmail.com")
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
