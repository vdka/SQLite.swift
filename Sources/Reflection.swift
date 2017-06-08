//
//  Reflection.swift
//  SQLiteYourself
//
//  Created by Ethan Jackwitz on 6/8/17.
//

import Foundation

struct Metadata : MetadataType {
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

func getPropertyDetails() -> []
