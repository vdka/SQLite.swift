
```swift
extension User {
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
        first_name TEXT NOT NULL,
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
let u3 = User(firstName: "Constancia", lastName: "McKinstry", age: 33, email: "cmckinstry2@state.gov")
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
```

