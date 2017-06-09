
```swift
import SQLiteYourself
import Foundation

struct User: Equatable {
    let firstName: String
    let lastName: String
    let age: Float
    let email: String
}

var databasePath: String = {
    return (Bundle(for: type(of: self)).bundlePath as NSString).appendingPathComponent("db.sqlite")
}()

let db = try! DB.open(path: databasePath)

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

for user in users {
    try! db.exec("INSERT INTO users (first_name, last_name, age, email) VALUES (?, ?, ?, ?)", params: user.firstName, user.lastName, user.age, user.email)
}

var rows = try! db.query("SELECT first_name, last_name, age, email FROM users ORDER BY age ASC")

for (row, expectedUser) in zip(rows, users.sorted(by: { $0.0.age < $0.1.age })) {
    let user = row.scan(User.self)

    assert(user == expectedUser)
}

let govEmployee = try! db.queryFirst("SELECT first_name, last_name, age, email FROM users WHERE email LIKE '%@%.gov'").scan(User.self)
assert(govEmployee == u3)

let namesOf30YearOlds = try! db.query("SELECT first_name, last_name FROM users WHERE age > 30 AND age < 40").map({ $0.scan((String, String).self) })
print(namesOf30YearOlds)
```