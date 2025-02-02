//
//  DuckDB
//  https://github.com/duckdb/duckdb-swift
//
//  Copyright © 2018-2023 Stichting DuckDB Foundation
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to
//  deal in the Software without restriction, including without limitation the
//  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
//  sell copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in
//  all copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
//  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
//  IN THE SOFTWARE.

protocol PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DBTypeID { get }
}

extension Bool: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DBTypeID { .boolean }
}

extension Int8: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DBTypeID { .tinyint }
}

extension Int16: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DBTypeID { .smallint }
}

extension Int32: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DBTypeID { .integer }
}

extension Int64: PrimitiveDatabaseValue  {
  static var representedDatabaseTypeID: DBTypeID { .bigint }
}

extension UInt8: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DBTypeID { .utinyint }
}

extension UInt16: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DBTypeID { .usmallint }
}

extension UInt32: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DBTypeID { .uinteger }
}

extension UInt64: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DBTypeID { .ubigint }
}

extension Float: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DBTypeID { .float }
}

extension Double: PrimitiveDatabaseValue {
  static var representedDatabaseTypeID: DBTypeID { .double }
}
