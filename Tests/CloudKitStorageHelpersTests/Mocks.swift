//
//  Mocks.swift
//  CloudKitStorageHelpersTests
//
//  Created by James Robinson on 4/6/20.
//

import Foundation
import CloudKitMocks
import CloudKitStorageHelpers

extension UploadableThing: CloudKitUploadable {}

extension DownloadableThing: CloudKitDownloadable {}

extension CloudKitMockContainer: CloudKitContainer {}

extension CloudKitMockDatabase: CloudKitDatabase {
    public func addOperation(_ operation: CloudKitOperation) {
        guard let op = operation as? CloudKitMockOperation else { return }
        op.database = self
        op.start()
    }
}

extension CloudKitMockOperation: CloudKitOperation {}

extension CloudKitMockFetch: CloudKitFetchRecordsOperation {}

extension CloudKitMockPush: CloudKitModifyRecordsOperation {}
