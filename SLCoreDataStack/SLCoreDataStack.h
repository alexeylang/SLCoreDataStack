//
//  SLCoreDataStack.h
//
//  The MIT License (MIT)
//  Copyright (c) 2013 Oliver Letterer, Sparrow-Labs
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in
//  all copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//  THE SOFTWARE.
//

#import <CoreData/CoreData.h>
#import <Foundation/Foundation.h>

extern NSString * const SLCoreDataStackWillPerformMigrationStep;
extern NSString * const SLCoreDataStackDidPerformMigrationStep;

/** User info keys for migration notifications; the values for these keys are numbers for model versions. */
extern NSString * const SLSourceModelVersionKey;
extern NSString * const SLTargetModelVersionKey;
/** User info key for SLCoreDataStackDidPerformMigrationStep; the value contains URL for temporary database. */
extern NSString * const SLTemporaryDataStoreURL;


extern NSString *const SLCoreDataStackErrorDomain;
extern NSString *const SLCoreDataStackWillMergeChangesNotification;
extern NSString *const SLCoreDataStackDidMergeChangesNotification;

enum {
    SLCoreDataStackMappingModelNotFound = 1,
    SLCoreDataStackManagedObjectModelNotFound
};



@interface SLCoreDataStack : NSObject

@property (nonatomic, readonly) NSURL *dataStoreURL;

@property (nonatomic, strong) NSManagedObjectModel *managedObjectModel;
@property (nonatomic, strong) NSPersistentStoreCoordinator *persistentStoreCoordinator;

@property (nonatomic, strong) NSManagedObjectContext *mainThreadManagedObjectContext;
@property (nonatomic, strong) NSManagedObjectContext *backgroundThreadManagedObjectContext;

/**
 Merge policies which will be applied to mainThreadManagedObjectContext and backgroundThreadManagedObjectContext.
 */
@property (nonatomic, readonly) id mainThreadMergePolicy;
@property (nonatomic, readonly) id backgroundThreadMergePolicy;

/**
 Return the name for your CoreData model here.
 
 @warning Must be overwritten.
 */
@property (nonatomic, readonly) NSString *managedObjectModelName;

/**
 Returns a unique shared instance for the calling class.
 */
+ (instancetype)sharedInstance;

/**
 The root URL in which the database will be stored. Default is NSLibraryDirectory.
 */
@property (nonatomic, readonly) NSURL *databaseRootURL;

/**
 The bundle, in with the momd file and migrations are stored.
 */
@property (nonatomic, readonly) NSBundle *bundle;

@property (nonatomic, readonly) BOOL requiresMigration;

/**
 Returns YES if any concrete subclass requires a migration that has been registered with +[SLCoreDataStack registerSubclass:].
 */
+ (BOOL)subclassesRequireMigration;

/**
 Registers a concrete subclass
 */
+ (void)registerConcreteSubclass:(Class)subclass;

/**
 Runs each available migration on its own an a different thread.
 */
+ (void)migrateSubclassesWithProgressHandler:(void(^)(SLCoreDataStack *currentMigratingSubclass))progressHandler
                           completionHandler:(dispatch_block_t)completionHandler;

/**
 Releases existent managed object contexts, persistent store and persistent store coordinator.
 */
- (void)removePersistentStoreCoordinator;

@end
