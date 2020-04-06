//
//  CloudKit+FileUploader.swift
//  Final Finance
//
//  Created by James Robinson on 2/8/20.
//  Copyright © 2020 LeadDev Creations, LLC. All rights reserved.
//

#if canImport(Combine) && canImport(CryptoKit) && canImport(CloudKit)
import Foundation
import CloudKit
import Combine
import CryptoKit
import CloudStorageCore

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
extension UploadError {
    
    public init(error: CKError) {
        switch error.code {
        case .alreadyShared:        self = .unauthorized
        case .assetFileModified:    self = .serviceUnavailable
        case .assetFileNotFound:    self = .noData
        case .assetNotAvailable:    self = .serviceUnavailable
        case .badContainer:         self = .unauthorized
        case .badDatabase:          self = .development("Cannot upload to that database")
        case .constraintViolation:  self = .unauthorized
        case .incompatibleVersion:  self = .serviceUnavailable
        case .internalError:        self = .serviceUnavailable
        case .invalidArguments:     self = .development("Invalid arguments")
        case .limitExceeded:        self = .development("Requested more than 400 items or 2 MB of record data")
        case .managedAccountRestricted: self = .unauthorized
        case .missingEntitlement:   self = .development("The app is missing a required entitlement.")
        case .networkFailure:       self = .networkUnavailable
        case .networkUnavailable:   self = .networkUnavailable
        case .notAuthenticated:     self = .notAuthenticated
        case .operationCancelled:   self = .cancelled
        case .partialFailure:       self = .init(partialFailure: error)
        case .participantMayNeedVerification: self = .unauthorized
        case .permissionFailure:    self = .unauthorized
        case .quotaExceeded:        self = .serviceUnavailable
        case .referenceViolation:   self = .development("Reference violation.")
        case .requestRateLimited:   self = .serviceUnavailable
        case .serverRecordChanged:  self = .serviceUnavailable
        case .serverRejectedRequest: self = .serviceUnavailable
        case .serverResponseLost:   self = .serviceUnavailable
        case .serviceUnavailable:   self = .serviceUnavailable
        case .tooManyParticipants:  self = .unauthorized
        case .userDeletedZone:      self = .unauthorized
        case .zoneBusy:             self = .serviceUnavailable
        case .zoneNotFound:
            guard let zoneName = error.recordZoneName else { fallthrough }
            self = .zoneNotFound(named: zoneName)
        default:                    self = .unknown
        }
    }
    
    /// Creates an `UploadError` value from the given partial `error`.
    private init(partialFailure error: CKError) {
        guard error.code == .partialFailure else {
            self = .development("Wrongly received error as partial failure: \(error)")
            return
        }
        
        let errors = error.userInfo[CKPartialErrorsByItemIDKey] as! [CKRecord.ID: CKError]
        if errors.count == 1 {
            self = .init(error: errors.first!.value)
            return
        }
        
        var result = [UUID: UploadError]()
        
        for (key, error) in errors {
            let id = UUID(uuidString: key.recordName)!
            result[id] = UploadError(error: error)
        }
        
        self = .multiple(result)
    }
    
}

@available(watchOS 3.0, *)
extension CKError {
    
    public var recordZoneName: String? {
        let description = self.userInfo["ServerErrorDescription"] as! String
        // Ye will find the word wrapped in swaddling quotes
        let quot: Character = "'"
        return String(description.split(separator: quot).dropFirst().first!)
    }
    
}

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public final class CloudKitFileUploader<Uploadable>: FileUploader where Uploadable: CloudKitUploadable {
    
    public static func uploadFile(_ file: Uploadable,
                                  encryptingWithKey encryptionKey: SymmetricKey?) throws -> CloudKitFileUploader<Uploadable> {
        return CloudKitFileUploader(file: file,
                                    encryptionKey: encryptionKey)
    }
    
    private let encryptionKey: SymmetricKey?
    private let file: Uploadable
    
    fileprivate init(file: Uploadable, encryptionKey: SymmetricKey?) {
        self.file = file
        self.encryptionKey = encryptionKey
    }
    
    public func receive<S>(subscriber: S) where S: Subscriber, UploadError == S.Failure, UploadProgress == S.Input {
        let subscription =
            CloudKitUploadSubscription(
                subscriber: subscriber,
                file: file,
                encryptionKey: encryptionKey
            )
        subscriber.receive(subscription: subscription)
    }
    
}

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
final class CloudKitUploadSubscription<SubscriberType, Uploadable>: Subscription
    where SubscriberType: Subscriber,
    UploadError == SubscriberType.Failure,
    UploadProgress == SubscriberType.Input,
    Uploadable: CloudKitUploadable {
    
    private var subscriber: SubscriberType?
    private var databaseScope: CKDatabase.Scope
    private let file: Uploadable
    private let encryptionKey: SymmetricKey?
    private var uploadOperation: Uploadable.ContainerType.Database.PushOperationType?
    /// The most recent upload progress event.
    public private(set) var latestProgress: UploadProgress
    
    private let defaultContainer = Uploadable.ContainerType.default()
    
    private var database: Uploadable.ContainerType.Database {
        defaultContainer.database(with: databaseScope)
    }
    
    fileprivate init(subscriber: SubscriberType,
                     database: CKDatabase.Scope = .private,
                     file: Uploadable,
                     encryptionKey: SymmetricKey?) {
        self.subscriber = subscriber
        self.databaseScope = database
        self.file = file
        self.encryptionKey = encryptionKey
        self.latestProgress = UploadProgress(completedBytes: 0,
                                             totalBytes: file.payload?.count ?? 0)
    }
    
    public func request(_ demand: Subscribers.Demand) {
        guard uploadOperation == nil else { return }
        start()
    }
    
    private func start() {
        uploadOperation?.cancel()
        do {
            let (record, temporaryURL) = try CKRecord.from(file, encryptingWith: encryptionKey)
            let operation = Uploadable.ContainerType.Database
                .PushOperationType(
                    recordsToSave: [record],
                    recordIDsToDelete: nil)
            operation.savePolicy = .changedKeys
            self.uploadOperation = operation
            
            operation.modifyRecordsCompletionBlock = { [weak self] (_, recordIDs, error) in
                guard let strongSelf = self else { return }
                if let error = error as? CKError {
                    // Failure...
                    return strongSelf.handleFailure(ckError: error)
                    
                } // Success
                if let strongSelf = self {
                    strongSelf.latestProgress.completedBytes = 100
                    _ = strongSelf.subscriber?.receive(strongSelf.latestProgress)
                }
                
                do {
                    // Delete the temporary file
                    try FileManager.default.removeItem(at: temporaryURL)
                } catch {
                    print("Failed to delete temporary file at \(temporaryURL.path): \(error)")
                }
                strongSelf.handleSuccess()
            }
            operation.perRecordProgressBlock = { [weak self] (record, fractionCompleted) in
                print("[CloudKitUploadSubscription] Uploading file \(self?.file.id.uuidString ?? "<null_self>"): \(fractionCompleted * 100) percent completed")
                if var progress = self?.latestProgress {
                    progress.completedBytes = Int(Double(progress.totalBytes) * fractionCompleted)
                    self?.latestProgress = progress
                    _ = self?.subscriber?.receive(self!.latestProgress)
                }
            }
            
            self.database.addOperation(operation)
            
        } catch let error as UploadError {
            self.handleFailure(error: error)
            
        } catch {
            fatalError("Caught unexpected error: \(error)")
        }
    }
    
    public func cancel() {
        handleFailure(error: .cancelled)
        uploadOperation?.cancel()
        uploadOperation = nil
    }
    
    private func handleFailure(ckError: CKError) {
        let error = UploadError(error: ckError)
        #if DEBUG
        print("[CloudKitUploadSubscription] Handling CloudKit error (\(error)) as: \(ckError)")
        #endif
        handleFailure(error: error)
    }
    
    private func handleFailure(error: UploadError) {
        if case .zoneNotFound(let zoneName) = error {
            print("[CloudKitUploadSubscription] Zone '\(zoneName)' was not found. Let's create it!")
            // Create the zone...
            return createZone(named: zoneName) { [weak self] (error) in
                // Zone created!
                if let error = error {
                    self?.handleFailure(ckError: error)
                } else {
                    // ... now retry
                    self?.start()
                }
            }
        }
        
        subscriber?.receive(completion: .failure(error))
        subscriber = nil
    }
    
    private func handleSuccess() {
        #if DEBUG
        print("[CloudKitUploadSubscription] Upload complete.")
        #endif
        
        latestProgress.completedBytes = latestProgress.totalBytes
        _ = subscriber?.receive(latestProgress)
        subscriber?.receive(completion: .finished)
        subscriber = nil
    }
    
    /// Creates a new zone in the private database with the user as owner.
    private func createZone(named zoneName: String, completion: @escaping (CKError?) -> Void) {
        let id = CKRecordZone.ID(zoneName: zoneName, ownerName: CKCurrentUserDefaultName)
        let zone = CKRecordZone(zoneID: id)
        
        let op = CKModifyRecordZonesOperation(recordZonesToSave: [zone])
        op.modifyRecordZonesCompletionBlock = { (_, zoneIDs, error) in
            if let zoneID = zoneIDs?.first {
                print("[CloudKitUploadSubscription] Created a new record zone named '\(zoneID.zoneName)'")
            }
            completion(error as? CKError)
        }
        
        defaultContainer.privateCloudDatabase.addOperation(op)
    }
    
}

@available(watchOS 3.0, *)
extension CKRecordZone.ID {
    
    /// The zone in which file attachments are kept.
    @available(OSX 10.12, iOS 10.0, tvOS 10.0, *)
    public static var defaultAttachmentsZone: CKRecordZone.ID {
        CKRecordZone.ID(zoneName: "Attachments", ownerName: CKCurrentUserDefaultName)
    }
    
}

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
private extension CKRecord {
    
    /// A `CKRecord` to use when uploading the file data.
    static func from<U>(_ file: U, encryptingWith encKey: SymmetricKey?) throws -> (record: CKRecord, fileURL: URL) where U: CloudKitUploadable {
        guard var data = file.payload else { throw UploadError.noData }
        
        if let encryptionKey = encKey {
            data = try ChaChaPoly.seal(data, using: encryptionKey).combined
        }
        
        let recordID = file.recordID
        let fileRecord = CKRecord(recordType: U.recordType, recordID: recordID)
        
        let attachmentsDirectory = FileManager.default
            .temporaryDirectory
            .appendingPathComponent("Attachment Upload Cache", isDirectory: true)
        
        let fileURL = attachmentsDirectory
            .appendingPathComponent(file.id.uuidString, isDirectory: false)
            .appendingPathExtension("uploading")
        
        do {
            try FileManager.default
                .createDirectory(at: attachmentsDirectory,
                                 withIntermediateDirectories: true)
            try data.write(to: fileURL)
        } catch {
            print("Could not write to \(fileURL.path): \(error)")
            throw error
        }
        
        fileRecord[.payloadKey] = CKAsset(fileURL: fileURL)
        if let size = try? FileManager.default.size(ofFileAt: fileURL) {
            fileRecord[.fileSizeKey] = NSNumber(value: size)
        }
        return (fileRecord, fileURL)
    }
    
}

// MARK: CloudKit Protocols



// MARK: Uploadable

@available(watchOS 3.0, *)
public protocol CloudKitUploadable: Uploadable where Metadata: CloudKitDownloadable {
    static var recordType: CKRecord.RecordType { get }
}

@available(watchOS 3.0, *)
extension CloudKitUploadable {
    public typealias ContainerType = Metadata.ContainerType
    public var recordID: CKRecord.ID { metadata.recordID }
}

// MARK: Container

@available(watchOS 3.0, *)
public protocol CloudKitContainer: AnyObject where Self == DefaultType {
    associatedtype Database: CloudKitDatabase
    associatedtype DefaultType: CloudKitContainer
    static func `default`() -> DefaultType
    @available(OSX 10.12, iOS 10.0, tvOS 10.0, *)
    func database(with databaseScope: CKDatabase.Scope) -> Database
}

@available(OSX 10.12, iOS 10.0, tvOS 10.0, watchOS 3.0, *)
extension CloudKitContainer {
    public var privateCloudDatabase: Database { database(with: .private) }
    public var publicCloudDatabase: Database { database(with: .public) }
    public var sharedCloudDatabase: Database { database(with: .shared) }
}

@available(watchOS 3.0, *)
extension CKContainer: CloudKitContainer {
    public typealias DefaultType = CKContainer
    public typealias Database = CKDatabase
}

// MARK: Database

@available(watchOS 3.0, *)
public protocol CloudKitDatabase {
    associatedtype FetchOperationType: CloudKitFetchRecordsOperation
    associatedtype PushOperationType: CloudKitModifyRecordsOperation
    associatedtype DeleteOperationType: CloudKitModifyRecordsOperation
    func addOperation(_ operation: CloudKitOperation)
}

@available(watchOS 3.0, *)
extension CKDatabase: CloudKitDatabase {
    public typealias FetchOperationType = CKFetchRecordsOperation
    public typealias PushOperationType = CKModifyRecordsOperation
    public typealias DeleteOperationType = CKModifyRecordsOperation
    
    public func addOperation(_ operation: CloudKitOperation) {
        if let op = operation as? CKDatabaseOperation {
            self.add(op)
        } else {
            operation.start()
        }
    }
}

// MARK: Push/Fetch Operations

public protocol CloudKitOperation: AnyObject {
    func start()
    func cancel()
}

@available(watchOS 3.0, *)
extension CKDatabaseOperation: CloudKitOperation {}

@available(watchOS 3.0, *)
public protocol CloudKitModifyRecordsOperation: CloudKitOperation {
    
    init()
    var recordsToSave: [CKRecord]? { get set }
    var recordIDsToDelete: [CKRecord.ID]? { get set }
    
    var perRecordProgressBlock: ((CKRecord, Double) -> Void)? { get set }
    var perRecordCompletionBlock: ((CKRecord, Error?) -> Void)? { get set }
    var modifyRecordsCompletionBlock: (([CKRecord]?, [CKRecord.ID]?, Error?) -> Void)? { get set }
    
    /// The policy to apply when the server contains a newer version of a specific record.
    ///
    /// Each record has a change tag that allows the server to track when that record was saved. When you
    /// save a record, CloudKit compares the change tag in your local copy of the record with the one on the
    /// server. If the two tags do not match—meaning that there is a potential conflict—the server uses the value
    /// in this property to determine how to proceed.
    ///
    /// The default value of this property is `.ifServerRecordUnchanged`. If you intend to change the
    /// value of this property, do so before executing the operation or submitting the operation object to a queue.
    var savePolicy: CKModifyRecordsOperation.RecordSavePolicy { get set }
    
    /// The relative amount of importance for granting system resources to the operation.
    ///
    /// Service levels affect the priority with which an operation object is given access to system resources such
    /// as CPU time, network resources, disk resources, and so on. Operations with a higher quality of service
    /// level are given greater priority over system resources so that they may perform their task more quickly.
    /// You use service levels to ensure that operations responding to explicit user requests are given priority over
    /// less critical work.
    ///
    /// This property reflects the minimum service level needed to execute the operation effectively. The default
    /// value of this property is `.background` and you should leave that value in place whenever possible.
    /// When changing the service level, use the minimum level that is appropriate for executing the corresponding
    /// task. For example, if the user initiates a task and is waiting for it to finish, assign the value `.userInteractive`
    /// to this property. The system may give the operation a higher service level to the operation if the resources
    /// are available to do so. For additional information, see Prioritize Work with Quality of Service Classes in Energy
    /// Efficiency Guide for iOS Apps and Prioritize Work at the Task Level in Energy Efficiency Guide for Mac Apps.
    var qualityOfService: QualityOfService { get set }
    
}

@available(watchOS 3.0, *)
extension CloudKitModifyRecordsOperation {
    public init(recordsToSave: [CKRecord]?, recordIDsToDelete: [CKRecord.ID]?) {
        self.init()
        self.recordsToSave = recordsToSave
        self.recordIDsToDelete = recordIDsToDelete
    }
}

@available(watchOS 3.0, *)
extension CKModifyRecordsOperation: CloudKitModifyRecordsOperation {}
#endif
