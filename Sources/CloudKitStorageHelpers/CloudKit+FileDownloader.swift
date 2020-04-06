//
//  CloudKit+FileDownloader.swift
//  CloudStorage
//
//  Created by James Robinson on 3/4/20.
//

import Foundation
import Combine
import CryptoKit
import CloudKit
import CloudStorage

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
extension DownloadError {
    
    public init(error: CKError) {
        switch error.code {
        case .assetFileNotFound:    self = .itemNotFound
        case .assetNotAvailable:    self = .serviceUnavailable
        case .badContainer:         self = .unauthorized
        case .badDatabase:          self = .development("Cannot download from that database")
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
        case .referenceViolation:   self = .itemNotFound
        case .requestRateLimited:   self = .serviceUnavailable
        case .serviceUnavailable:   self = .serviceUnavailable
        case .serverRejectedRequest: self = .serviceUnavailable
        case .unknownItem:          self = .itemNotFound
        case .userDeletedZone:      self = .itemNotFound
        case .zoneBusy:             self = .serviceUnavailable
        case .zoneNotFound:         self = .itemNotFound
        default:                    self = .unknown
        }
    }
    
    /// Creates a `DownloadError` value from the given partial `error`.
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
        
        var result = [UUID: DownloadError]()
        
        for (key, error) in errors {
            let id = UUID(uuidString: key.recordName)!
            result[id] = DownloadError(error: error)
        }
        
        self = .multiple(result)
    }
    
}

// MARK: Publisher

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public final class CloudKitFileDownloader<Downloadable>: FileDownloader where Downloadable: CloudKitDownloadable {
    
    public static func downloadFile(_ file: Downloadable,
                                    to outputDirectory: URL,
                                    decryptingUsing decryptionKey: SymmetricKey?) throws -> CloudKitFileDownloader<Downloadable> {
        // Local path
        let cachedItemURL: URL
        
        if outputDirectory.hasDirectoryPath { // If it's a directory, add file name
            cachedItemURL = outputDirectory
                .appendingPathComponent(file.id.uuidString, isDirectory: false)
                .appendingPathExtension(file.fileExtension ?? "")
        } else { // If it's a file, use that.
            cachedItemURL = outputDirectory
        }
        
        return CloudKitFileDownloader<Downloadable>(
            outputFileURL: cachedItemURL,
            recordID: file.recordID,
            decryptionKey: decryptionKey)
    }
    
    public static func deleteFile(_ file: Downloadable) throws -> CloudKitFileDeleter<Downloadable> {
        return CloudKitFileDeleter(recordID: file.recordID)
    }
    
    public let recordID: CKRecord.ID
    private let decryptionKey: SymmetricKey?
    private let outputFileURL: URL
    
    public init(outputFileURL: URL,
                recordID: CKRecord.ID,
                decryptionKey: SymmetricKey?) {
        self.outputFileURL = outputFileURL
        self.recordID = recordID
        self.decryptionKey = decryptionKey
    }
    
    public func receive<S>(subscriber: S) where S: Subscriber, DownloadError == S.Failure, DownloadProgress == S.Input {
        let subscription =
            CloudKitDownloadSubscription<S, Downloadable.ContainerType>(
                subscriber: subscriber,
                recordID: recordID,
                outputFileURL: outputFileURL,
                decryptionKey: decryptionKey
            )
        subscriber.receive(subscription: subscription)
    }
    
}

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public final class CloudKitFileDeleter<Deletable>: FileDeleter where Deletable: CloudKitDownloadable {
    
    public let recordID: CKRecord.ID
    
    public init(recordID: CKRecord.ID) {
        self.recordID = recordID
    }
    
    public func receive<S>(subscriber: S) where S: Subscriber, DownloadError == S.Failure, Never == S.Input {
        let subscription =
            CloudKitDeletionSubscription<S, Deletable.ContainerType>(
                subscriber: subscriber,
                recordID: recordID
            )
        subscriber.receive(subscription: subscription)
    }
    
}

// MARK: Download Subscription

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public final class CloudKitDownloadSubscription<SubscriberType, ContainerType>: Subscription
    where SubscriberType: Subscriber,
    DownloadError == SubscriberType.Failure,
    DownloadProgress == SubscriberType.Input,
    ContainerType: CloudKitContainer {
    
    private var subscriber: SubscriberType?
    private var databaseScope: CKDatabase.Scope
    private var recordID: CKRecord.ID
    private var downloadOperation: ContainerType.Database.FetchOperationType?
    private var outputFileURL: URL
    private let decryptionKey: SymmetricKey?
    
    public private(set) var latestProgress: DownloadProgress
    
    private let defaultContainer = ContainerType.default()
    
    private var database: ContainerType.Database {
        defaultContainer.database(with: databaseScope)
    }
    
    fileprivate init(subscriber: SubscriberType,
                     database: CKDatabase.Scope = .private,
                     recordID: CKRecord.ID,
                     outputFileURL: URL,
                     decryptionKey: SymmetricKey?) {
        self.subscriber = subscriber
        self.databaseScope = database
        self.recordID = recordID
        self.downloadOperation = nil
        self.outputFileURL = outputFileURL
        self.decryptionKey = decryptionKey
        self.latestProgress = DownloadProgress(completedBytes: 0, totalBytes: nil)
    }
    
    public func request(_ demand: Subscribers.Demand) {
        guard downloadOperation == nil else { return }
        // CloudKit uses its own temporary download directory
        downloadOperation = ContainerType.Database.FetchOperationType(recordIDs: [recordID])
        
        guard let op = self.downloadOperation else { return }
        
        // Observe download
        op.fetchRecordsCompletionBlock = { [weak self] (recordsByID, error) in
            if let error = error as? CKError {
                self?.handleFailure(ckError: error)
            } else if let recordsByID = recordsByID {
                if let strongSelf = self {
                    strongSelf.latestProgress.completedBytes = 100
                    _ = strongSelf.subscriber?.receive(strongSelf.latestProgress)
                }
                self?.handleSuccess(recordsByID)
            }
        }
        op.perRecordCompletionBlock = { [weak self] (record, id, error) in
            if let error = error as? CKError {
                print("[CloudKitDownloadSubscription] Failed to download file \(id?.recordName ?? "<null_name>"): \(error)")
                self?.handleFailure(ckError: error)
            }
        }
        op.perRecordProgressBlock = { [weak self] (recordID, fractionCompleted) in
            print("[CloudKitDownloadSubscription] Downloading file \(recordID.recordName): \(fractionCompleted * 100) percent completed.")
            if let strongSelf = self {
                strongSelf.latestProgress.completedBytes = Int(100 * fractionCompleted)
                _ = strongSelf.subscriber?.receive(strongSelf.latestProgress)
            }
        }
        
        database.addOperation(downloadOperation!)
    }
    
    public func cancel() {
        handleFailure(error: .cancelled)
        downloadOperation?.cancel()
        downloadOperation = nil
    }
    
    private func handleFailure(ckError: CKError) {
        let err = DownloadError(error: ckError)
        #if DEBUG
        print("[CloudKitDownloadSubscription] Handling CloudKit error (\(err)) as: \(ckError)")
        #endif
        handleFailure(error: err)
    }
    
    private func handleFailure(error: DownloadError) {
        #if DEBUG
        print("[CloudKitDownloadSubscription] Download failed: \(error)")
        #endif
        
        subscriber?.receive(completion: .failure(error))
        subscriber = nil
    }
    
    private lazy var workQueue: DispatchQueue = {
        let label = "CloudKitDownloadSubscription.workQueue(\(UUID().uuidString))"
        return DispatchQueue(label: label, qos: .utility)
    }()
    
    private func handleSuccess(_ result: [CKRecord.ID: CKRecord]) {
        #if DEBUG
        print("[CloudKitDownloadSubscription] Download complete.")
        #endif
        
        guard result.count == 1 else {
            return handleFailure(error: .development("Received more than one record: \(result)"))
        }
        
        if let total = latestProgress.totalBytes {
            latestProgress.completedBytes = total
        }
        
        workQueue.async {
            guard let asset = result.first?.value[.payloadKey] as? CKAsset else {
                let error = DownloadError
                    .development("The asset or its payload came back `nil` in record: \(String(describing: result.first?.value))")
                return self.handleFailure(error: error)
            }
            let downloadedFileURL = asset.fileURL!
            
            do {
                if let key = self.decryptionKey { // Decrypt!
                    let rawData = try Data(contentsOf: downloadedFileURL)
                    let box = try ChaChaPoly.SealedBox(combined: rawData)
                    let decryptedData = try ChaChaPoly.open(box, using: key)
                    
                    try? FileManager.default // Remove existing output, if any
                        .removeItem(at: self.outputFileURL)
                    try FileManager.default // Prepare our destination directory
                        .createDirectory(at: self.outputFileURL.deletingLastPathComponent(),
                                         withIntermediateDirectories: true)
                    #if !os(macOS)
                    try decryptedData // Create decrypted payload at the new location
                        .write(to: self.outputFileURL,
                               options: .completeFileProtection)
                    #else
                    try decryptedData // Create decrypted payload at the new location
                        .write(to: self.outputFileURL)
                    #endif
                    // Don't delete the original. CloudKit manages that.
                    
                } else { // Just move the downloaded file
                    try? FileManager.default // Remove existing cache, if any
                        .removeItem(at: self.outputFileURL)
                    try FileManager.default // Prepare our destination directory
                        .createDirectory(at: self.outputFileURL.deletingLastPathComponent(),
                                         withIntermediateDirectories: true)
                    try FileManager.default // Copy the downloaded file (CK manages its data)
                        .copyItem(at: downloadedFileURL, to: self.outputFileURL)
                }
                self.subscriber?.receive(completion: .finished)
                self.subscriber = nil
                
            } catch let error as CocoaError {
                print("[CloudKitDownloadSubscription] Failed to copy from '\(downloadedFileURL.path)' to output file '\(self.outputFileURL.path)': \(error)")
                self.handleFailure(error: .disk(error))
            } catch let error as CryptoKitError {
                print("[CloudKitDownloadSubscription] Failed to decrypt downloaded payload: \(error)")
                self.handleFailure(error: .decryption(error))
            } catch {
                print("[CloudKitDownloadSubscription] Unknown error while finishing download operation: \(error)")
                self.handleFailure(error: .unknown)
            }
        }
    }
    
}

// MARK: Deletion Subscription

@available(OSX 10.15, iOS 13.0, tvOS 13.0, watchOS 6.0, *)
public final class CloudKitDeletionSubscription<SubscriberType, ContainerType>: Subscription
    where SubscriberType: Subscriber,
    DownloadError == SubscriberType.Failure,
    Never == SubscriberType.Input,
    ContainerType: CloudKitContainer {
    
    private var subscriber: SubscriberType?
    private var databaseScope: CKDatabase.Scope
    private var recordID: CKRecord.ID
    private var deleteOperation: ContainerType.Database.DeleteOperationType?
    
    private let defaultContainer = ContainerType.default()
    
    private var database: ContainerType.Database {
        defaultContainer.database(with: databaseScope)
    }
    
    fileprivate init(subscriber: SubscriberType,
                     database: CKDatabase.Scope = .private,
                     recordID: CKRecord.ID) {
        self.subscriber = subscriber
        self.databaseScope = database
        self.recordID = recordID
        self.deleteOperation = nil
    }
    
    public func request(_ demand: Subscribers.Demand) {
        guard deleteOperation == nil else { return }
        deleteOperation = ContainerType.Database
            .DeleteOperationType(recordsToSave: nil, recordIDsToDelete: [recordID])
        
        guard let op = self.deleteOperation else { return }
        
        // Observe download
        op.modifyRecordsCompletionBlock = { [weak self] (_, _, error) in
            if let error = error as? CKError {
                self?.handleFailure(ckError: error)
            } else {
                self?.handleSuccess()
            }
        }
        op.perRecordCompletionBlock = { [weak self] (record, error) in
            if let error = error as? CKError {
                let id = record.recordID
                print("[CloudKitDeletionSubscription] Failed to delete file \(id.recordName): \(error)")
                self?.handleFailure(ckError: error)
            }
        }
        
        database.addOperation(deleteOperation!)
    }
    
    public func cancel() {
        handleFailure(error: .cancelled)
        deleteOperation?.cancel()
        deleteOperation = nil
    }
    
    private func handleFailure(ckError: CKError) {
        let err = DownloadError(error: ckError)
        #if DEBUG
        print("[CloudKitDeletionSubscription] Handling CloudKit error (\(err)) as: \(ckError)")
        #endif
        handleFailure(error: err)
    }
    
    private func handleFailure(error: DownloadError) {
        #if DEBUG
        print("[CloudKitDeletionSubscription] Deletion failed: \(error)")
        #endif
        
        subscriber?.receive(completion: .failure(error))
        subscriber = nil
    }
    
    private func handleSuccess() {
        #if DEBUG
        print("[CloudKitDeletionSubscription] Deletion complete.")
        #endif
        
        self.subscriber?.receive(completion: .finished)
        self.subscriber = nil
    }
    
}

// MARK: - CloudKit Protocols



// MARK: Downloadable

/// A type that conforms to this protocol may be used by `CloudKitFileDownloader`s
/// to retrieve data from CloudKit.
@available(watchOS 3.0, *)
public protocol CloudKitDownloadable: Downloadable {
    /// The type of CloudKit container to use.  Use `CKContainer` for normal CloudKit capabilities.
    associatedtype ContainerType: CloudKitContainer
    /// The ID of the CloudKit record associated with the file data.
    var recordID: CKRecord.ID { get }
}

extension String {
    
    public static let payloadKey = "payload"
    public static let fileSizeKey = "fileSize"
    
}

// MARK: Fetch Operation

@available(watchOS 3.0, *)
public protocol CloudKitFetchRecordsOperation: CloudKitOperation {
    
    init(recordIDs: [CKRecord.ID])
    
    var perRecordProgressBlock: ((CKRecord.ID, Double) -> Void)? { get set }
    var perRecordCompletionBlock: ((CKRecord?, CKRecord.ID?, Error?) -> Void)? { get set }
    var fetchRecordsCompletionBlock: (([CKRecord.ID: CKRecord]?, Error?) -> Void)? { get set }
    
}

@available(watchOS 3.0, *)
extension CKFetchRecordsOperation: CloudKitFetchRecordsOperation {}
