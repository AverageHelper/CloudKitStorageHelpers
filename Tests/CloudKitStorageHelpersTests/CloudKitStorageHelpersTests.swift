import XCTest
import Combine
import CryptoKit
import CloudKit
import CloudKitMocks
import CloudStorage
import CloudKitStorageHelpers

final class CloudKitStorageHelpersTests: XCTestCase {
    
    static var allTests: [(String, (CloudKitStorageHelpersTests) -> () throws -> ())] = [
        ("testUploadProgressFraction", testUploadProgressFraction),
        ("testUploadNotAuthenticated", testUploadNotAuthenticated),
        ("testCloudKitUploadSuccess", testCloudKitUploadSuccess),
        ("testCloudKitDownloadSuccess", testCloudKitDownloadSuccess),
        ("testCloudKitFileDeleteNonexistent", testCloudKitFileDeleteNonexistent),
    ]
    
    override func setUp() {
        CloudKitMock.defaultContainer.wipe() // Clear mock Storage
        CloudKitMock.currentUserID = nil // Sign out of mock
    }
    
    override static func tearDown() {
        // After all tests are done...
        CloudKitMock.defaultContainer.wipe() // Clear mock Storage
        CloudKitMock.currentUserID = nil // Sign out of mock
    }
    
    func testUploadProgressFraction() {
        var prog = UploadProgress(completedBytes: 100, totalBytes: 100)
        XCTAssertEqual(prog.fractionCompleted, 1, accuracy: 0.001)
        prog.completedBytes = 50
        XCTAssertEqual(prog.fractionCompleted, 0.5, accuracy: 0.001)
    }
    
    func testUploadNotAuthenticated() {
        let file = DownloadableThing()
        guard let payload = file.storage else {
            return XCTFail("SANITY FAIL: New DownloadableThing's storage is nil.")
        }
        
        let uploadShouldFail = expectation(description: "CloudKit should fail to upload.")
        var upload: AnyCancellable? = try! CloudKitFileUploader
            .uploadFile(payload, encryptingWithKey: nil)
            .sink(receiveCompletion: { (completion) in
                uploadShouldFail.fulfill()
                switch completion {
                case .finished:
                    XCTFail("User is signed in with id \(CloudKitMock.currentUserID ?? "<null>")")
                    
                case .failure(let error):
                    if case .notAuthenticated = error { /* pass */ } else {
                        XCTFail("Wrong error thrown: \(error)")
                    }
                }
            }, receiveValue: { progress in
                XCTFail("Received progress: \(progress)")
            })
        
        wait(for: [uploadShouldFail], timeout: 2)
        if upload != nil {
            upload = nil
        }
    }
    
    func runUploader<U>(_ uploader: U) where U: FileUploader {
        let progress = expectation(description: "\(U.self) operation should show progress")
        progress.assertForOverFulfill = false
        let shouldUpload = expectation(description: "\(U.self) operation should complete")
        
        var upload: AnyCancellable? = uploader
            .sink(receiveCompletion: { (completion) in
                if case .failure(let error) = completion {
                    XCTFail("\(error)")
                }
                shouldUpload.fulfill()
                
            }, receiveValue: { uploadProgress in
                progress.fulfill()
                XCTAssertGreaterThanOrEqual(uploadProgress.fractionCompleted, .zero)
            })
        
        wait(for: [progress, shouldUpload], timeout: 3, enforceOrder: true)
        if upload != nil {
            upload = nil
        }
    }
    
    func testCloudKitUploadSuccess() {
        let file = DownloadableThing()
        guard let payload = file.storage else {
            return XCTFail("SANITY FAIL: New DownloadableThing's storage was nil")
        }
        CloudKitMock.currentUserID = "someUser"
        
        do {
            let uploader = try CloudKitFileUploader.uploadFile(payload, encryptingWithKey: nil)
            runUploader(uploader)
            
            let uploadedRecord = try? CloudKitMock
                .defaultContainer
                .privateCloudDatabase
                .fetchRecord(withID: file.recordID)
            if let uploadedAsset = uploadedRecord?["payload"] as? CKAsset,
                let url = uploadedAsset.fileURL {
                XCTAssertEqual(file.storage!.payload!, try? Data(contentsOf: url))
            } else {
                XCTFail("No asset returned from CloudKit mock.")
            }
            XCTAssertNotNil(file.storage?.payload,
                            "File needs to retain its local data; it hasn't been confirmed yet.")
            
        } catch {
            return XCTFail("\(error)")
        }
    }
    
    func runDownloader<D, U>(_ downloader: D,
                             after uploader: U,
                             expectingData expectedPayload: Data) where D: FileDownloader, U: FileUploader {
        // We first upload...
        // (We cannot easily mock this, as FirebaseFileUploader encrypts items as it works)
        let progress = expectation(description: "\(U.self) operation should show progress")
        progress.assertForOverFulfill = false
        let shouldUpload = expectation(description: "\(U.self) operation should complete")
        
        var upload: AnyCancellable? = uploader
            .receive(on: DispatchQueue.main)
            .sink(receiveCompletion: { (completion) in
                if case .failure(let error) = completion {
                    XCTFail("\(error)")
                }
                shouldUpload.fulfill()
                
            }, receiveValue: { uploadProgress in
                progress.fulfill()
                XCTAssertGreaterThanOrEqual(uploadProgress.fractionCompleted, .zero)
            })
        
        wait(for: [progress, shouldUpload], timeout: 2, enforceOrder: true)
        if upload != nil {
            upload = nil
        }
        
        // Now we download. Compare the differences.
        let downloadExpectation = expectation(description: "\(D.self) operation should show progress")
        downloadExpectation.assertForOverFulfill = false
        let shouldDownload = expectation(description: "\(D.self) operation should complete")
        
        var download: AnyCancellable? = downloader
            .receive(on: DispatchQueue.main)
            .sink(receiveCompletion: { (completion) in
                if case .failure(let error) = completion {
                    XCTFail("\(error)")
                }
                shouldDownload.fulfill()
                
            }, receiveValue: { downloadProgress in
                downloadExpectation.fulfill()
                XCTAssertGreaterThanOrEqual(downloadProgress.fractionCompleted ?? 0, .zero)
            })
        
        wait(for: [downloadExpectation, shouldDownload], timeout: 3, enforceOrder: true)
        if download != nil {
            download = nil
        }
    }
    
    func testCloudKitDownloadSuccess() throws {
        CloudKitMock.currentUserID = "someUser"
        let file = DownloadableThing()
        guard let payload = file.storage else {
            return XCTFail("SANITY FAIL: New DownloadableThing's storage was nil")
        }
        let encKey = SymmetricKey(size: .bits256)
        
        let uploader: CloudKitFileUploader<UploadableThing>
        do {
            uploader = try CloudKitFileUploader.uploadFile(payload, encryptingWithKey: encKey)
        } catch {
            return XCTFail("\(error)")
        }
        
        let downloadURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(file.id.uuidString, isDirectory: false)
            .appendingPathExtension(file.fileExtension ?? "download")
        try? FileManager.default.removeItem(at: downloadURL)
        try FileManager.default
            .createDirectory(at: downloadURL.deletingLastPathComponent(),
                             withIntermediateDirectories: true)
        
        let downloader: CloudKitFileDownloader<DownloadableThing>
        do {
            downloader = try CloudKitFileDownloader.downloadFile(file, to: downloadURL, decryptingUsing: encKey)
        } catch {
            return XCTFail("\(error)")
        }

        XCTAssertFalse(FileManager.default.fileExists(atPath: downloadURL.path))
        runDownloader(downloader, after: uploader, expectingData: file.storage!.payload!)
        XCTAssertTrue(FileManager.default.fileExists(atPath: downloadURL.path))
        
        try FileManager.default.removeItem(at: downloadURL)
    }
    
    func testCloudKitFileDeleteNonexistent() {
        CloudKitMock.currentUserID = "someUser"

        let file = DownloadableThing()
        let deletionExpec = expectation(description: "CloudKit file deletion should fail for nonexistent file.")

        let deleter: CloudKitFileDeleter<DownloadableThing>

        do {
            deleter = try CloudKitFileDownloader.deleteFile(file)
        } catch {
            return XCTFail("\(error)")
        }

        var deletion: AnyCancellable? = deleter
            .receive(on: DispatchQueue.main)
            .sink(receiveCompletion: { (completion) in
                switch completion {
                case .failure(DownloadError.itemNotFound): break
                case .failure(let error):
                    XCTFail("Unexpected error '\(error)'")
                case .finished:
                    XCTFail("Deletion should fail on nonexistent file.")
                }
                deletionExpec.fulfill()
            }, receiveValue: { _ in })

        wait(for: [deletionExpec], timeout: 3)
        if deletion != nil {
            deletion = nil
        }
    }
    
}
