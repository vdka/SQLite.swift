
import Foundation

public enum Column {
    case text(String)
    case integer(Int)
    case real(Double)
    case blob(Data)
}

extension Column: CustomStringConvertible {

    public var description: String {
        switch self {
        case .text(let s):
            return s

        case .integer(let i):
            return i.description

        case .real(let d):
            return d.description

        case .blob(let d):
            return "<bytes x \(d.count)>"
        }
    }
}

public protocol SQLDataType {
    static var size: Int { get }

    static func get(from column: Column) -> Self
    var sqlColumnValue: Column { get }
}

extension SQLDataType {

    public static var size: Int {
        return MemoryLayout<Self>.size
    }
}

extension Int: SQLDataType {
    public static func get(from column: Column) -> Int {
        guard case .integer(let v) = column else {
            fatalError()
        }
        return v
    }

    public var sqlColumnValue: Column {
        return Column.integer(self)
    }
}

extension String: SQLDataType {
    public static func get(from column: Column) -> String {
        guard case .text(let v) = column else {
            fatalError()
        }
        return v
    }

    public var sqlColumnValue: Column {
        return Column.text(self)
    }
}

extension Double: SQLDataType {
    public static func get(from column: Column) -> Double {
        guard case .real(let v) = column else {
            fatalError()
        }
        return v
    }

    public var sqlColumnValue: Column {
        return Column.real(self)
    }
}

extension Float: SQLDataType {
    public static func get(from column: Column) -> Float {
        guard case .real(let v) = column else {
            fatalError()
        }
        return Float(v)
    }

    public var sqlColumnValue: Column {
        return Column.real(Double(self))
    }
}

extension Bool: SQLDataType {
    public static func get(from column: Column) -> Bool {
        guard case .integer(let v) = column else {
            fatalError()
        }
        return v != 0
    }

    public var sqlColumnValue: Column {
        return Column.integer(self ? 1 : 0)
    }
}

extension Date: SQLDataType {
    public static func get(from column: Column) -> Date {
        guard case .real(let v) = column else {
            fatalError()
        }
        return Date(timeIntervalSince1970: v)
    }

    public var sqlColumnValue: Column {
        return Column.real(self.timeIntervalSince1970)
    }
}

extension Data: SQLDataType {
    public static func get(from column: Column) -> Data {
        guard case .blob(let v) = column else {
            fatalError()
        }
        return v
    }

    public var sqlColumnValue: Column {
        return Column.blob(self)
    }
}

extension URL: SQLDataType {
    public static func get(from column: Column) -> URL {
        guard case .text(let v) = column else {
            fatalError()
        }
        return URL(string: v)!
    }

    public var sqlColumnValue: Column {
        return Column.text(self.absoluteString)
    }
}

extension UUID: SQLDataType {
    public static func get(from column: Column) -> UUID {
        guard case .text(let v) = column else {
            fatalError()
        }
        return UUID(uuidString: v)!
    }

    public var sqlColumnValue: Column {
        return Column.text(self.uuidString)
    }
}

