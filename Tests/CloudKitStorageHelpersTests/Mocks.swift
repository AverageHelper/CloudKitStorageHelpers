//
//  CloudKitMocks.swift
//  CloudKitStorageHelpersTests
//
//  Created by James Robinson on 3/5/20.
//

#if canImport(Combine) && canImport(CryptoKit)
import Foundation
import CloudKit
import CloudStorage
import CloudKitStorageHelpers

@available(OSX 10.12, iOS 10.0, tvOS 10.0, *)
struct UploadableThing: Uploadable, Equatable {
    var payload: Data? = Data(repeating: 5, count: 64)
    var metadata: DownloadableThing
}

@available(OSX 10.12, iOS 10.0, tvOS 10.0, *)
final class DownloadableThing: Downloadable, Equatable {
    
    var recordIdentifier = UUID()
    var id = UUID()
    var fileExtension: String? = "testfile"
    lazy var storage: UploadableThing? = UploadableThing(metadata: self)
    
    static func == (lhs: DownloadableThing, rhs: DownloadableThing) -> Bool {
        return lhs.storage == rhs.storage &&
            lhs.fileExtension == rhs.fileExtension &&
            lhs.id == rhs.id &&
            lhs.recordIdentifier == rhs.recordIdentifier
    }
    
}

@available(OSX 10.12, iOS 10.0, tvOS 10.0, *)
extension UploadableThing: CloudKitUploadable {
    static var recordType: CKRecord.RecordType { "UploadableThing" }
}

@available(OSX 10.12, iOS 10.0, tvOS 10.0, *)
extension DownloadableThing: CloudKitDownloadable {
    typealias ContainerType = CloudKitMockContainer
    
    var recordID: CKRecord.ID {
        CKRecord.ID(recordName: id.uuidString, zoneID: Self.zoneID)
    }
    
    static var zoneID: CKRecordZone.ID {
        CKRecordZone.ID(zoneName: "UploadableThings", ownerName: CKCurrentUserDefaultName)
    }
}

// MARK: - Mocks

@available(OSX 10.12, iOS 10.0, tvOS 10.0, watchOS 3.0, *)
enum CloudKitMock {
    static var currentUserID: String?
    static var defaultContainer = CloudKitMockContainer()
}

@available(OSX 10.12, iOS 10.0, tvOS 10.0, watchOS 3.0, *)
final class CloudKitMockContainer: CloudKitContainer {
    typealias Database = CloudKitMockDatabase
    typealias DefaultType = CloudKitMockContainer
    
    static func `default`() -> CloudKitMockContainer {
        return CloudKitMock.defaultContainer
    }
    
    private static var databases = [CKDatabase.Scope: CloudKitMockDatabase]()
    
    func database(with databaseScope: CKDatabase.Scope) -> CloudKitMockDatabase {
        if let db = Self.databases[databaseScope] {
            return db
        }
        Self.databases[databaseScope] = CloudKitMockDatabase(scope: databaseScope)
        return Self.databases[databaseScope]!
    }
    
    func wipe() {
        Self.databases.forEach { $0.value.wipe() }
        Self.databases.removeAll()
    }
}

private struct RecordIdentifier: Hashable {
    let id: CKRecord.ID
    let type: CKRecord.RecordType
    
    init(for record: CKRecord) {
        id = record.recordID
        type = record.recordType
    }
}

/// A mock for a CloudKit database.
@available(OSX 10.12, iOS 10.0, tvOS 10.0, *)
final class CloudKitMockDatabase: CloudKitDatabase {
    
    typealias FetchOperationType = CloudKitMockFetch
    typealias PushOperationType = CloudKitMockPush
    typealias DeleteOperationType = CloudKitMockPush
    
    let scope: CKDatabase.Scope
    private var uploadedData = [RecordIdentifier: CKRecord]()
    
    init(scope: CKDatabase.Scope) {
        self.scope = scope
    }
    
    private var storedAssetData = [RecordIdentifier: [String: URL]]()
    
    private var assetStorageDir: URL {
        let scopeDescription: String
        switch scope {
        case .private: scopeDescription = "Private"
        case .public: scopeDescription = "Public"
        case .shared: scopeDescription = "Shared"
        @unknown default: scopeDescription = "CKDatabase.Scope.\(scope.rawValue)"
        }
        return FileManager.default
            .temporaryDirectory
            .appendingPathComponent("CloudKitMock-AssetStorage", isDirectory: true)
            .appendingPathComponent(scopeDescription, isDirectory: true)
    }
    
    func wipe() {
        uploadedData.removeAll()
        try? FileManager.default.removeItem(at: assetStorageDir)
    }
    
    private func assertAuthenticated() throws {
        guard CloudKitMock.currentUserID != nil else {
            throw CKError(.notAuthenticated)
        }
    }
    
    func pushRecord(_ record: CKRecord) throws {
        try assertAuthenticated()
        let id = RecordIdentifier(for: record)
        uploadedData[id] = record
        storedAssetData[id] =
            try record.assets().store(in: assetStorageDir)
    }
    
    func fetchRecord(withID recordID: CKRecord.ID) throws -> CKRecord? {
        try assertAuthenticated()
        guard let id = uploadedData.keys.first(where: { $0.id == recordID }) else {
            throw CKError(.unknownItem)
        }
        guard var record = uploadedData[id] else { throw CKError(.unknownItem) }
        guard let assetData = storedAssetData[id] else { return record }
        assetData.copy(into: &record)
        return record
    }
    
    func deleteRecord(withID recordID: CKRecord.ID) throws {
        try assertAuthenticated()
        guard let id = uploadedData.keys.first(where: { $0.id == recordID }) else {
            throw CKError(.unknownItem)
        }
        uploadedData.removeValue(forKey: id)
        guard let assetData = storedAssetData.removeValue(forKey: id)?.values else {
            throw CKError(.unknownItem)
        }
        for fileURL in assetData {
            do {
                try FileManager.default.removeItem(at: fileURL)
            } catch {
                print("[CloudKitMockDatabase] Failed to delete mock asset data at \(fileURL): \(error)")
                if #available(OSX 10.13, iOS 11.3, tvOS 11.3, *) {
                    throw CKError(.assetNotAvailable)
                } else {
                    throw CKError(.assetFileNotFound)
                }
            }
        }
    }
    
    func addOperation(_ operation: CloudKitOperation) {
        guard let op = operation as? CloudKitMockOperation else { return }
        op.database = self
        op.start()
    }
}

@available(OSX 10.12, iOS 10.0, tvOS 10.0, *)
class CloudKitMockOperation: CloudKitOperation {
    
    var database: CloudKitMockDatabase?
    private(set) var isRunning: Bool = false
    private(set) var isCancelled: Bool = false
    
    func start() {
        guard !isRunning else { return }
        isRunning = true
        isCancelled = false
    }
    
    func cancel() {
        isRunning = false
        isCancelled = true
    }
    
}


@available(OSX 10.12, iOS 10.0, tvOS 10.0, *)
final class CloudKitMockFetch: CloudKitMockOperation, CloudKitFetchRecordsOperation {
    
    var delay: TimeInterval = 1
    var fetchError: Error?
    
    var perRecordProgressBlock: ((CKRecord.ID, Double) -> Void)?
    var perRecordCompletionBlock: ((CKRecord?, CKRecord.ID?, Error?) -> Void)?
    var fetchRecordsCompletionBlock: (([CKRecord.ID : CKRecord]?, Error?) -> Void)?
    var recordIDs: [CKRecord.ID]
    
    init(recordIDs: [CKRecord.ID]) {
        self.recordIDs = recordIDs
    }
    
    private var progressQueue: OperationQueue?
    private var workingRecord: CKRecord.ID?
    private var errorsSent = [CKRecord.ID: CKError]()
    
    private func advanceWorkingRecord() {
        if let workingRecord = self.workingRecord,
            let index = recordIDs.firstIndex(of: workingRecord),
            index + 1 < recordIDs.endIndex {
            self.workingRecord = recordIDs[index + 1]
        } else {
            self.workingRecord = nil
        }
    }
    
    override func start() {
        super.start()
        self.workingRecord = recordIDs.first
        if let error = fetchError {
            simulateFailure(delay: delay, error: error)
        } else {
            simulateFetch(completionDelay: delay)
        }
    }
    
    func simulateFailure(delay seconds: TimeInterval = 0, error: Error) {
        let progressItemCount = seconds > 0
            ? 3 // Do 3 things first
            : 0 // Just get on with it
        
        progressQueue = OperationQueue()
        let work = { () -> Void in
            let totalTime = seconds * 0.98
            let waitTime = totalTime / Double(progressItemCount)
            Thread.sleep(forTimeInterval: waitTime)
            
            guard !self.isCancelled else { return }
            guard let workingRecord = self.workingRecord else { return }
            self.perRecordProgressBlock?(workingRecord, 0.25)
            
            self.advanceWorkingRecord()
        }
        progressQueue?.maxConcurrentOperationCount = 1
        progressQueue?.qualityOfService = .utility
        var operations = [Operation]()
        if progressItemCount <= 0 {
            operations.append(BlockOperation(block: work))
        } else {
            for _ in 0..<progressItemCount {
                operations.append(BlockOperation(block: work))
            }
        }
        progressQueue?.addOperations(operations, waitUntilFinished: false)
        
        DispatchQueue.main.asyncAfter(deadline: .now() + seconds) {
            if self.isCancelled {
                let err = CKError(.operationCancelled)
                self.fetchRecordsCompletionBlock?(nil, err)
                return
            }
            // Fail
            self.fetchRecordsCompletionBlock?(nil, error)
        }
    }
    
    func simulateFetch(completionDelay seconds: TimeInterval = 0) {
        let progressItemCount = seconds > 0
            ? 3 // Do 3 things first
            : 0 // Just get on with it
        
        progressQueue = OperationQueue()
        let work = { () -> Void in
            let totalTime = seconds * 0.98
            let waitTime = totalTime / Double(progressItemCount)
            Thread.sleep(forTimeInterval: waitTime / 2)
            
            guard !self.isCancelled else { return }
            guard let workingRecord = self.workingRecord else { return }
            self.perRecordProgressBlock?(workingRecord, 0.25)
            
            Thread.sleep(forTimeInterval: waitTime / 2)
            
            do {
                if let record = try self.database?.fetchRecord(withID: workingRecord) {
                    self.perRecordCompletionBlock?(record, workingRecord, nil)
                } else if !self.errorsSent.keys.contains(workingRecord) {
                    self.perRecordCompletionBlock?(nil, workingRecord, CKError(.unknownItem))
                }
            } catch {
                self.fetchError = error
                self.perRecordCompletionBlock?(nil, nil, error)
            }
            
            self.advanceWorkingRecord()
        }
        progressQueue?.maxConcurrentOperationCount = 1
        progressQueue?.qualityOfService = .utility
        var operations = [Operation]()
        if progressItemCount <= 0 {
            operations.append(BlockOperation(block: work))
        } else {
            for _ in 0..<progressItemCount {
                operations.append(BlockOperation(block: work))
            }
        }
        progressQueue?.addOperations(operations, waitUntilFinished: false)
        
        DispatchQueue.main.asyncAfter(deadline: .now() + seconds) {
            guard !self.isCancelled else {
                let err = CKError(.operationCancelled)
                self.fetchRecordsCompletionBlock?(nil, err)
                return
            }
            
            var records = [CKRecord.ID: CKRecord]()
            do {
                for id in self.recordIDs {
                    if let record = try self.database?.fetchRecord(withID: id) {
                        records[id] = record
                    } else if !self.errorsSent.keys.contains(id) {
                        self.perRecordCompletionBlock?(nil, self.workingRecord, CKError(.unknownItem))
                    }
                }
                // Succeed
                self.fetchRecordsCompletionBlock?(records, self.fetchError) // Make composite error
            } catch {
                self.fetchRecordsCompletionBlock?(nil, error)
            }
        }
    }
}

@available(OSX 10.12, iOS 10.0, tvOS 10.0, *)
final class CloudKitMockPush: CloudKitMockOperation, CloudKitModifyRecordsOperation {
    
    var delay: TimeInterval = 0
    var pushError: Error?
    
    var recordsToSave: [CKRecord]?
    var recordIDsToDelete: [CKRecord.ID]?
    
    var perRecordProgressBlock: ((CKRecord, Double) -> Void)?
    var perRecordCompletionBlock: ((CKRecord, Error?) -> Void)?
    var modifyRecordsCompletionBlock: (([CKRecord]?, [CKRecord.ID]?, Error?) -> Void)?
    var savePolicy: CKModifyRecordsOperation.RecordSavePolicy = .ifServerRecordUnchanged
    var qualityOfService: QualityOfService = .default
    
    override init() {
        super.init()
    }
    
    private var progressQueue: OperationQueue?
    private var workingRecord: CKRecord.ID?
    
    private func advanceWorkingRecord() {
        guard let workingRecord = self.workingRecord else { return }
        
        let recordIDsToSave = (recordsToSave ?? []).map(\.recordID)
        if let saveIndex = recordIDsToSave.firstIndex(of: workingRecord),
            saveIndex + 1 < recordIDsToSave.endIndex {
            self.workingRecord = recordIDsToSave[saveIndex + 1]
            
        } else if let deleteIndex = (recordIDsToDelete ?? []).firstIndex(of: workingRecord),
            deleteIndex + 1 < (recordIDsToDelete ?? []).endIndex {
            self.workingRecord = (recordIDsToDelete ?? [])[deleteIndex + 1]
            
        } else {
            self.workingRecord = nil
        }
    }
    
    override func start() {
        super.start()
        self.workingRecord = recordsToSave?.first?.recordID
        if let error = pushError {
            simulateFailure(delay: delay, error: error)
        } else {
            simulatePush(completionDelay: delay)
        }
    }
    
    func simulateFailure(delay seconds: TimeInterval = 0, error: Error) {
        let progressItemCount = seconds > 0
            ? 3 // Do 3 things first
            : 0 // Just get on with it
        
        // Run progress callbacks before completion callbacks
        progressQueue = OperationQueue()
        let work = { () -> Void in
            let totalTime = seconds * 0.98
            let waitTime = totalTime / Double(progressItemCount)
            Thread.sleep(forTimeInterval: waitTime)
            
            guard !self.isCancelled else { return }
            guard let workingRecord = self.workingRecord else { return }
            if let record = self.recordsToSave?.first(where: { $0.recordID == workingRecord }) {
                self.perRecordCompletionBlock?(record, error)
            }
            
            self.advanceWorkingRecord()
        }
        progressQueue?.maxConcurrentOperationCount = 1
        progressQueue?.qualityOfService = .utility
        var operations = [Operation]()
        if progressItemCount <= 0 {
            operations.append(BlockOperation(block: work))
        } else {
            for _ in 0..<progressItemCount {
                operations.append(BlockOperation(block: work))
            }
        }
        progressQueue?.addOperations(operations, waitUntilFinished: false)
        
        DispatchQueue.main.asyncAfter(deadline: .now() + seconds) {
            if self.isCancelled {
                let err = CKError(.operationCancelled)
                self.modifyRecordsCompletionBlock?(nil, nil, err)
                return
            }
            // Fail
            self.modifyRecordsCompletionBlock?(nil, nil, error)
        }
    }
    
    func simulatePush(completionDelay seconds: TimeInterval = 0) {
        let progressItemCount = seconds > 0
            ? 3 // Do 3 things first
            : 0 // Just get on with it
        
        progressQueue = OperationQueue()
        let work = { () -> Void in
            let totalTime = seconds * 0.98
            let waitTime = totalTime / Double(progressItemCount)
            Thread.sleep(forTimeInterval: waitTime)
            
            guard !self.isCancelled else { return }
            guard let workingRecord = self.workingRecord else { return }
            if let record = self.recordsToSave?.first(where: { $0.recordID == workingRecord }) {
                self.perRecordCompletionBlock?(record, nil)
            }
            
            self.advanceWorkingRecord()
        }
        progressQueue?.maxConcurrentOperationCount = 1
        progressQueue?.qualityOfService = .utility
        var operations = [Operation]()
        if progressItemCount <= 0 {
            operations.append(BlockOperation(block: work))
        } else {
            for _ in 0..<progressItemCount {
                operations.append(BlockOperation(block: work))
            }
        }
        progressQueue?.addOperations(operations, waitUntilFinished: false)
        
        DispatchQueue.main.asyncAfter(deadline: .now() + seconds) {
            guard !self.isCancelled else {
                let err = CKError(.operationCancelled)
                self.modifyRecordsCompletionBlock?(nil, nil, err)
                return
            }
            
            do {
                for record in self.recordsToSave ?? [] {
                    try self.database?.pushRecord(record)
                }
                for id in self.recordIDsToDelete ?? [] {
                    try self.database?.deleteRecord(withID: id)
                }
                // Succeed
                self.modifyRecordsCompletionBlock?(self.recordsToSave, self.recordIDsToDelete, self.pushError)
            } catch {
                self.modifyRecordsCompletionBlock?(nil, nil, error)
            }
        }
    }
    
}

private extension CKRecord {
    
    /// Returns all `CKAssets` associated with the record, and their key in the record.
    func assets() -> [String: CKAsset] {
        var result = [String: CKAsset]()
        
        for key in allKeys() {
            guard let asset = self[key] as? CKAsset else { continue }
            result[key] = asset
        }
        
        return result
    }
    
}

private extension Dictionary where Key == String, Value == CKAsset {
    
    /// Stores the contents of the receiver in the given parent directory. Returns a new dictionary containing
    /// the keys and new file URLs with the assets' contents.
    func store(in parentDirectory: URL) throws -> [String: URL] {
        try FileManager.default.createDirectory(at: parentDirectory,
                                                withIntermediateDirectories: true)
        var result = [String: URL]()
        
        for (key, asset) in self {
            guard let assetURL = asset.fileURL else { continue }
            let fileURL = parentDirectory
                .appendingPathComponent(key, isDirectory: false)
                .appendingPathExtension("ckasset")
            try FileManager.default.copyItem(at: assetURL, to: fileURL)
            result[key] = fileURL
        }
        
        return result
    }
    
}

private extension Dictionary where Key == String, Value == URL {
    
    func copy(into record: inout CKRecord) {
        for (key, url) in self {
            record[key] = CKAsset(fileURL: url)
        }
    }
    
}
#endif
