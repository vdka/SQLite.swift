

//
// This file is derived from the ABI notes available as part of the Swift Open Source project.
//     https://github.com/apple/swift/blob/master/docs/ABI.rst
//

protocol AnyExtensions {}

extension AnyExtensions {
    static func write(_ value: Any, to storage: UnsafeMutableRawPointer) {
        guard let this = value as? Self else {
            fatalError("Internal logic error")
        }
        storage.assumingMemoryBound(to: self).initialize(to: this)
    }
}

/// Magic courtesy of Zewo/Reflection
func extensions(of type: Any.Type) -> AnyExtensions.Type {
    struct Extensions : AnyExtensions {}
    var extensions: AnyExtensions.Type = Extensions.self
    withUnsafePointer(to: &extensions) { pointer in
        UnsafeMutableRawPointer(mutating: pointer).assumingMemoryBound(to: Any.Type.self).pointee = type
    }
    return extensions
}

protocol MetadataType {
    var pointer: UnsafeRawPointer { get }
    static var kind: Metadata.Kind? { get }
}

extension MetadataType {
    var valueWitnessTable: ValueWitnessTable {
        return ValueWitnessTable(pointer: pointer.assumingMemoryBound(to: UnsafeRawPointer.self).advanced(by: -1).pointee)
    }

    var kind: Metadata.Kind {
        return Metadata.Kind(flag: pointer.assumingMemoryBound(to: Int.self).pointee)
    }

    init(pointer: UnsafeRawPointer) {
        self = unsafeBitCast(pointer, to: Self.self)
    }

    init?(type: Any.Type) {
        self.init(pointer: unsafeBitCast(type, to: UnsafeRawPointer.self))
        if let kind = type(of: self).kind, kind != self.kind {
            return nil
        }
    }
}

struct Metadata: MetadataType {
    var pointer: UnsafeRawPointer

    init(type: Any.Type) {
        self.pointer = unsafeBitCast(type, to: UnsafeRawPointer.self)
    }
}

// https://github.com/apple/swift/blob/swift-3.0-branch/include/swift/ABI/MetadataKind.def
extension Metadata {
    static let kind: Kind? = nil

    enum Kind {
        case `struct`
        case `enum`
        case optional
        case opaque
        case tuple
        case function
        case existential
        case metatype
        case objCClassWrapper
        case existentialMetatype
        case foreignClass
        case heapLocalVariable
        case heapGenericLocalVariable
        case errorObject
        case `class`
        init(flag: Int) {
            switch flag {
            case 1: self = .struct
            case 2: self = .enum
            case 3: self = .optional
            case 8: self = .opaque
            case 9: self = .tuple
            case 10: self = .function
            case 12: self = .existential
            case 13: self = .metatype
            case 14: self = .objCClassWrapper
            case 15: self = .existentialMetatype
            case 16: self = .foreignClass
            case 64: self = .heapLocalVariable
            case 65: self = .heapGenericLocalVariable
            case 128: self = .errorObject
            default: self = .class
            }
        }
    }
}

// https://github.com/apple/swift/blob/master/lib/IRGen/ValueWitness.h
struct ValueWitnessTable {
    var pointer: UnsafeRawPointer

    private var alignmentMask: Int {
        return 0x0FFFF
    }

    var size: Int {
        return pointer.assumingMemoryBound(to: _ValueWitnessTable.self).pointee.size
    }

    var align: Int {
        return (pointer.assumingMemoryBound(to: _ValueWitnessTable.self).pointee.align & alignmentMask) + 1
    }

    var stride: Int {
        return pointer.assumingMemoryBound(to: _ValueWitnessTable.self).pointee.stride
    }
}

struct _ValueWitnessTable {
    let destroyBuffer: Int
    let initializeBufferWithCopyOfBuffer: Int
    let projectBuffer: Int
    let deallocateBuffer: Int
    let destroy: Int
    let initializeBufferWithCopy: Int
    let initializeWithCopy: Int
    let assignWithCopy: Int
    let initializeBufferWithTake: Int
    let initializeWithTake: Int
    let assignWithTake: Int
    let allocateBuffer: Int
    let initializeBufferWithTakeOrBuffer: Int
    let destroyArray: Int
    let initializeArrayWithCopy: Int
    let initializeArrayWithTakeFrontToBack: Int
    let initializeArrayWithTakeBackToFront: Int
    let size: Int
    let align: Int
    let stride: Int
}


extension Metadata {
    // https://github.com/apple/swift/blob/master/docs/ABI.rst#tuple-metadata
    struct Tuple: MetadataType {
        static let kind: Kind? = .tuple
        var pointer: UnsafeRawPointer
    }
}

extension Metadata.Tuple {

    var numberOfElements: Int {
        let offset = 1
        let pointer = UnsafeRawPointer(self.pointer.assumingMemoryBound(to: Int.self).advanced(by: offset)).assumingMemoryBound(to: Int.self)
        return pointer.pointee
    }

    var elementTypes: [Any.Type] {
        let offset = 3
        let pointer = UnsafeRawPointer(self.pointer.assumingMemoryBound(to: Int.self).advanced(by: offset)).assumingMemoryBound(to: Int.self)

        var types: [Any.Type] = []

        for index in 0..<numberOfElements {
            let type = unsafeBitCast(pointer.advanced(by: 2 * index).pointee, to: Any.Type.self)
            types.append(type)
        }

        return types
    }

    var elementOffsets: [Int] {
        let offset = 3
        let pointer = UnsafeRawPointer(self.pointer.assumingMemoryBound(to: Int.self).advanced(by: offset)).assumingMemoryBound(to: Int.self)

        var offsets: [Int] = []

        for index in 0..<numberOfElements {
            offsets.append(pointer.advanced(by: 2 * index + 1).pointee)
        }

        return offsets
    }
}

extension Metadata {
    struct Struct: MetadataType {
        static let kind: Kind? = .struct
        var pointer: UnsafeRawPointer
    }
}

extension Metadata.Struct {
    var nominalTypeDescriptorOffset: Int {
        return 1
    }

    var nominalTypeDescriptorPointer: UnsafeRawPointer {
        let pointer = self.pointer.assumingMemoryBound(to: Int.self)
        let base = pointer.advanced(by: nominalTypeDescriptorOffset)
        return UnsafeRawPointer(base).advanced(by: base.pointee)
    }

    var fieldOffsets: [Int] {
        let offset = 3
        let base = UnsafeRawPointer(self.pointer.assumingMemoryBound(to: Int.self).advanced(by: offset)).assumingMemoryBound(to: Int.self)

        var offsets: [Int] = []

        for index in 0..<numberOfFields {
            offsets.append(base.advanced(by: index).pointee)
        }

        return offsets
    }

    // NOTE: The rest of the struct Metadata is stored on the NominalTypeDescriptor

    // NOTE(vdka): Not sure why but all the offset's mentioned in the ABI for Nominal Type Descriptors are off by 1. The pointer we have points to the mangled name offset.

    var mangledName: String { // offset 0

        let offset = nominalTypeDescriptorPointer.assumingMemoryBound(to: Int32.self).pointee
        let p = nominalTypeDescriptorPointer.advanced(by: numericCast(offset)).assumingMemoryBound(to: CChar.self)
        return String(cString: p)
    }

    var numberOfFields: Int { // offset 1

        let offset = 1
        return numericCast(nominalTypeDescriptorPointer.load(fromByteOffset: offset * halfword, as: Int32.self))
    }

    var fieldNames: [String] { // offset 3

        let offset = 3
        let base = nominalTypeDescriptorPointer.advanced(by: offset * halfword)

        let dataOffset = base.load(as: Int32.self)
        let fieldNamesPointer = base.advanced(by: numericCast(dataOffset))

        return Array(utf8Strings: fieldNamesPointer.assumingMemoryBound(to: CChar.self))
    }

    //
    // from ABI.rst:
    //
    // The field type accessor is a function pointer at offset 5. If non-null, the function takes a pointer to an
    //   instance of type metadata for the nominal type, and returns a pointer to an array of type metadata
    //   references for the types of the fields of that instance. The order matches that of the field offset vector
    //   and field name list.
    typealias FieldsTypeAccessor = @convention(c) (UnsafeRawPointer) -> UnsafePointer<UnsafeRawPointer>
    var fieldTypesAccessor: FieldsTypeAccessor? { // offset 4

        let offset = 4
        let base = nominalTypeDescriptorPointer.advanced(by: offset * halfword)

        let dataOffset = base.load(as: Int32.self)
        guard dataOffset != 0 else { return nil }

        let dataPointer = base.advanced(by: numericCast(dataOffset))
        return unsafeBitCast(dataPointer, to: FieldsTypeAccessor.self)
    }

    var fieldTypes: [Any.Type]? {
        guard let accessorFunction = fieldTypesAccessor else { return nil }

        var types: [Any.Type] = []
        for fieldIndex in 0..<numberOfFields {
            let pointer = accessorFunction(nominalTypeDescriptorPointer).advanced(by: fieldIndex).pointee
            let type = unsafeBitCast(pointer, to: Any.Type.self)
            types.append(type)
        }

        return types
    }
}


// MARK: - Helpers

let word = min(MemoryLayout<Int>.size, MemoryLayout<Int64>.size)
let halfword = word / 2

extension Array where Element == String {

    init(utf8Strings: UnsafePointer<CChar>) {
        var strings = [String]()
        var pointer = utf8Strings

        while true {
            let string = String(cString: pointer)
            strings.append(string)
            while pointer.pointee != 0 {
                pointer = pointer.advanced(by: 1)
            }
            pointer = pointer.advanced(by: 1)
            guard pointer.pointee != 0 else { break }
        }
        self = strings
    }
}

