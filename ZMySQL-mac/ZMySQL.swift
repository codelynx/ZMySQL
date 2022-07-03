//
//  ZMySQL.swift
//  ZMySQL
//
//  Created by Kaz Yoshikawa on 7/1/22.
//

import Foundation
import AppKit
import mysql

extension String: Error {
}

public class ZMySQLDatabase {
	public static let utf8mb4 = "utf8mb4"
	let mySQL: UnsafeMutablePointer<MYSQL>
	public init(socket: String, username: String, password: String, port: UInt32, database: String) {
		fatalError("not tested yet")
		let flags: UInt = 0
		guard let mySQL = mysql_init(nil) else { fatalError() }
		let result = mysql_real_connect(mySQL, nil, username, password, database, port, socket, flags)
		guard let result = result, result == mySQL else { fatalError() }
		self.mySQL = mySQL
		if mysql_set_character_set(mySQL, Self.utf8mb4) != 0 { fatalError() }
	}
	public init(host: String, port: UInt32, username: String, password: String, database: String) {
		guard let mySQL = mysql_init(nil) else { fatalError() }
		let result = mysql_real_connect(mySQL, host, username, password, database, port, nil, 0)
		if let message = mysql_error(mySQL) {
			print(String(cString: message))
		}
		assert(result == mySQL)
		if mysql_set_character_set(mySQL, Self.utf8mb4) != 0 { fatalError() }
		self.mySQL = mySQL
	}
	public func query(_ query: String) -> ZMySQLQuery {
		return ZMySQLQuery(database: self, query: query)
	}
	deinit {
		mysql_close(self.mySQL)
	}
	var error: String {
		return String(cString: mysql_error(mySQL))
	}
}

public class ZMySQLQuery {
	let database: ZMySQLDatabase
	var query: String
	public init(database: ZMySQLDatabase, query: String) {
		self.database = database
		self.query = query
	}
	public func execute() -> ZMySQLResult? {
		mysql_real_query(self.database.mySQL, self.query, UInt(strlen(self.query)))
		if let res = mysql_use_result(self.database.mySQL) {
			let metadata = mysql_result_metadata(res)
			print(metadata)
			return ZMySQLResult(query: self, res: res)
		}
		else {
			print(self.database.error)
		}
		return nil
	}
}

public class ZMySQLResult {
	let query: ZMySQLQuery
	var res: UnsafeMutablePointer<MYSQL_RES>
	init(query: ZMySQLQuery, res: UnsafeMutablePointer<MYSQL_RES>) {
		self.query = query
		self.res = res
	}
	deinit {
		mysql_free_result(UnsafeMutablePointer(self.res))
	}
	public func nextRow() -> ZMySQLRow? {
		if let row = mysql_fetch_row(self.res) {
			aaa()
			return ZMySQLRow(result: self, row: UnsafeMutablePointer<MYSQL_ROW>(OpaquePointer(row)))
		}
		return nil
	}
	private (set) lazy var columns: [ZMySQLColumn] = {
		let fields = mysql_fetch_fields(self.res)!
		return (0..<Int(self.numberOfColumns)).map { ZMySQLColumn(result: self, field: fields[Int($0)], index: $0) }
	}()
	private (set) lazy var columnDictionary: [String: ZMySQLColumn] = {
		return self.columns.reduce(into: [:]) { $0[$1.name] = $1 }
	}()
	public var numberOfColumns: Int {
		return Int(mysql_num_fields(self.res))
	}
	func aaa() {
		let numberOfFields = mysql_num_fields(self.res)
		let fields: [UnsafeMutablePointer<MYSQL_FIELD>] = (0..<numberOfFields).map { mysql_fetch_field_direct(self.res, $0) }
		print(fields)
	}
//	public func byteLengths() -> [Int] {
//		guard let lengths: UnsafeMutablePointer<UInt> = mysql_fetch_lengths(self.res) else { fatalError() }
//		return Array(UnsafeBufferPointer(start: lengths, count: self.numberOfColumns)).map { Int($0) }
//	}
}

public class ZMySQLRow: CustomStringConvertible {
	let result: ZMySQLResult
	let row: UnsafeMutablePointer<MYSQL_ROW>
	var lengths: [Int] // byte length of the data in that column
	init(result: ZMySQLResult, row: UnsafeMutablePointer<MYSQL_ROW>) {
		self.result = result
		self.row = row
		let numberOfColumns = Int(mysql_num_fields(self.result.res))
		guard let lengths: UnsafeMutablePointer<UInt> = mysql_fetch_lengths(self.result.res) else { fatalError() }
		self.lengths = Array(UnsafeBufferPointer(start: lengths, count: numberOfColumns)).map { Int($0) }
	}
	public var columns: [ZMySQLColumn] {
		return self.result.columns
	}
	subscript<T>(key: String) -> T? {
		if let column = self.result.columnDictionary[key] {
			let pointer = self.row[column.index]
			let length = self.lengths[column.index]
			let result = column.value(pointer: pointer, length: length) as? T
			print(result ?? "nil")
			return result
		}
		return nil
	}
	public var description: String {
		return self.result.columns.map { $0.name }.joined(separator: "\r")
	}
	var values: [Any?] {
		return self.result.columns.map { $0.value(pointer: self.row[$0.index], length: self.lengths[$0.index]) }
	}
}

extension enum_field_types: CustomStringConvertible {
	public var description: String {
		switch self {
		case MYSQL_TYPE_DECIMAL: return "MYSQL_TYPE_DECIMAL"
		case MYSQL_TYPE_TINY: return "MYSQL_TYPE_TINY"
		case MYSQL_TYPE_SHORT: return "MYSQL_TYPE_SHORT"
		case MYSQL_TYPE_LONG: return "MYSQL_TYPE_LONG"
		case MYSQL_TYPE_FLOAT: return "MYSQL_TYPE_FLOAT"
		case MYSQL_TYPE_DOUBLE: return "MYSQL_TYPE_DOUBLE"
		case MYSQL_TYPE_NULL: return "MYSQL_TYPE_NULL"
		case MYSQL_TYPE_TIMESTAMP: return "MYSQL_TYPE_TIMESTAMP"
		case MYSQL_TYPE_LONGLONG: return "MYSQL_TYPE_LONGLONG"
		case MYSQL_TYPE_INT24: return "MYSQL_TYPE_INT24"
		case MYSQL_TYPE_DATE: return "MYSQL_TYPE_DATE"
		case MYSQL_TYPE_TIME: return "MYSQL_TYPE_TIME"
		case MYSQL_TYPE_DATETIME: return "MYSQL_TYPE_DATETIME"
		case MYSQL_TYPE_YEAR: return "MYSQL_TYPE_YEAR"
		case MYSQL_TYPE_NEWDATE: return "MYSQL_TYPE_NEWDATE"  /**< Internal to MySQL. Not used in protocol */
		case MYSQL_TYPE_VARCHAR: return "MYSQL_TYPE_VARCHAR"
		case MYSQL_TYPE_BIT: return "MYSQL_TYPE_BIT"
		case MYSQL_TYPE_TIMESTAMP2: return "MYSQL_TYPE_TIMESTAMP2"
		case MYSQL_TYPE_DATETIME2: return "MYSQL_TYPE_DATETIME2"  /**< Internal to MySQL. Not used in protocol */
		case MYSQL_TYPE_TIME2: return "MYSQL_TYPE_TIME2"  /**< Internal to MySQL. Not used in protocol */
		case MYSQL_TYPE_TYPED_ARRAY: return "MYSQL_TYPE_TYPED_ARRAY"  /**< Used for replication only */
		case MYSQL_TYPE_INVALID: return "MYSQL_TYPE_INVALID"
		case MYSQL_TYPE_BOOL: return "MYSQL_TYPE_BOOL"  /**< Currently just a placeholder */
		case MYSQL_TYPE_JSON: return "MYSQL_TYPE_JSON"
		case MYSQL_TYPE_NEWDECIMAL: return "MYSQL_TYPE_NEWDECIMAL"
		case MYSQL_TYPE_ENUM: return "MYSQL_TYPE_ENUM"
		case MYSQL_TYPE_SET: return "MYSQL_TYPE_SET"
		case MYSQL_TYPE_TINY_BLOB: return "MYSQL_TYPE_TINY_BLOB"
		case MYSQL_TYPE_MEDIUM_BLOB: return "MYSQL_TYPE_MEDIUM_BLOB"
		case MYSQL_TYPE_LONG_BLOB: return "MYSQL_TYPE_LONG_BLOB"
		case MYSQL_TYPE_BLOB: return "MYSQL_TYPE_BLOB"
		case MYSQL_TYPE_VAR_STRING: return "MYSQL_TYPE_VAR_STRING"
		case MYSQL_TYPE_STRING: return "MYSQL_TYPE_STRING"
		case MYSQL_TYPE_GEOMETRY: return "MYSQL_TYPE_GEOMETRY"
		default: return "MYSQL_TYPE unknown: (\(self.rawValue))"
		}
	}
}

public class ZMySQLColumn {
	let result: ZMySQLResult
	let field: MYSQL_FIELD
	let name: String
	let index: Int
	init(result: ZMySQLResult, field: MYSQL_FIELD, index: Int) {
		self.result = result
		self.field = field
		self.name = String(cString: field.name)
		self.index = index
	}
	var isBinary: Bool {
		return self.field.flags & UInt32(BINARY_FLAG) != 0
	}
	func formatter(format: String) -> DateFormatter {
		let formatter = DateFormatter()
		formatter.dateFormat = format
		formatter.locale = Locale(identifier: "en_US_POSIX")
		formatter.calendar = Calendar(identifier: .gregorian)
		return formatter
	}
	func value(pointer: UnsafeRawPointer, length: Int) -> Any? {
		let data = Data(bytes: pointer, count: length)
		let string = String(bytes: data, encoding: .utf8)
		print(self.field.type.description)
		switch (self.field.type) {
		case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL:
			return string.flatMap { Decimal(string: $0) }
		case MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG:
			return string.flatMap { Int($0) }
		case MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE:
			return string.flatMap { Double($0) }
		case MYSQL_TYPE_NULL:
			return string
		case MYSQL_TYPE_TIMESTAMP:
			return string
		case MYSQL_TYPE_LONGLONG:
			return string.flatMap { Int64($0) }
		case MYSQL_TYPE_INT24:
			return string.flatMap { Int($0) }
		case MYSQL_TYPE_DATE:
			let formatter = self.formatter(format: "yyyy-MM-dd")
			let dateComponents = string.flatMap { formatter.date(from: $0) }.flatMap { formatter.calendar.dateComponents([.year, .month, .day], from: $0) }
			return dateComponents
		case MYSQL_TYPE_DATETIME:
			let formatter = self.formatter(format: "yyyy-MM-dd HH:mm:ss")
			let dateComponents = string.flatMap { formatter.date(from: $0) }.flatMap { formatter.calendar.dateComponents([.year, .month, .day, .hour, .minute, .second], from: $0) }
			return dateComponents
		case MYSQL_TYPE_TIME: // not 'HH:mm:ss' more like duration
			return string
		case MYSQL_TYPE_YEAR, MYSQL_TYPE_NEWDATE:
			return string
		case MYSQL_TYPE_VARCHAR:
			return string
		case MYSQL_TYPE_BIT:
			return string.flatMap { Int($0) }
		case MYSQL_TYPE_TIMESTAMP2:
			return string
		case MYSQL_TYPE_TYPED_ARRAY:
			return string
		case MYSQL_TYPE_INVALID:
			return string
		case MYSQL_TYPE_BOOL, MYSQL_TYPE_JSON, MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET:
			return string
		case MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB: // `text` type??
			return data
		case MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING:
			return string
		case MYSQL_TYPE_GEOMETRY: // TODO:
			return string
		default:
			fatalError()
		}
	}
}