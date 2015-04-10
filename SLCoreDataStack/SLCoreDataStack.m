//
//  SLCoreDataStack.m
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

#import "SLCoreDataStack.h"
#import <objc/runtime.h>
#import <stdio.h>


NSString * const SLCoreDataStackWillPerformMigrationStep = @"SLCoreDataStackWillPerformMigrationStep";
NSString * const SLCoreDataStackDidPerformMigrationStep = @"SLCoreDataStackDidPerformMigrationStep";

NSString * const SLSourceModelVersionKey = @"SLSourceModelVersionKey";
NSString * const SLTargetModelVersionKey = @"SLTargetModelVersionKey";
NSString * const SLTemporaryDataStoreURL = @"SLTemporaryDataStoreURL";

NSString * const SLCoreDataStackErrorDomain = @"SLCoreDataStackErrorDomain";
NSString * const SLCoreDataStackDidMergeChangesNotification = @"SLCoreDataStackDidMergeChangesNotification";


@interface SLCoreDataStack ()

@property (nonatomic, readonly) NSURL *_dataStoreRootURL;
@property (nonatomic, readonly) BOOL requiresMigration;
@property (nonatomic, strong) NSMutableArray *pendingDeletedObjects;

@end


@implementation SLCoreDataStack
@synthesize mainThreadManagedObjectContext = _mainThreadManagedObjectContext, backgroundThreadManagedObjectContext = _backgroundThreadManagedObjectContext;

#pragma mark - setters and getters

- (id)mainThreadMergePolicy
{
    return NSMergeByPropertyStoreTrumpMergePolicy;
}

- (id)backgroundThreadMergePolicy
{
    return NSMergeByPropertyObjectTrumpMergePolicy;
}

- (NSString *)managedObjectModelName
{
    [self doesNotRecognizeSelector:_cmd];
    return nil;
}

- (NSURL *)databaseRootURL
{
    NSString *applicationName = [[[NSBundle mainBundle] infoDictionary] valueForKey:(NSString *)kCFBundleNameKey];
    NSURL *appSupportURL = [[NSFileManager defaultManager] URLsForDirectory:NSApplicationSupportDirectory
                                                                  inDomains:NSUserDomainMask].lastObject;
    return [appSupportURL URLByAppendingPathComponent:applicationName];
}

- (NSURL *)dataStoreURL
{
    NSURL *dataStoreRootURL = self._dataStoreRootURL;
    NSString *dataStoreFileName = [NSString stringWithFormat:@"%@.sqlite", self.managedObjectModelName];

    return [dataStoreRootURL URLByAppendingPathComponent:dataStoreFileName];
}

- (NSURL *)_dataStoreRootURL
{
    NSURL *dataStoreRootURL = self.databaseRootURL;

    if (![[NSFileManager defaultManager] fileExistsAtPath:dataStoreRootURL.relativePath isDirectory:NULL]) {
        NSError *error = nil;
        [[NSFileManager defaultManager] createDirectoryAtPath:dataStoreRootURL.relativePath
                                  withIntermediateDirectories:YES
                                                   attributes:nil
                                                        error:&error];

        NSAssert(error == nil, @"error while creating dataStoreRootURL '%@':\n\nerror: \"%@\"", dataStoreRootURL, error);
    }

    return dataStoreRootURL;
}

- (NSBundle *)bundle
{
    return [NSBundle bundleForClass:self.class];
}

- (BOOL)requiresMigration
{
    return [self requiresMigrationFromDataStoreAtURL:self.dataStoreURL toDestinationModel:self.managedObjectModel];
}

- (BOOL)requiresMigrationFromDataStoreAtURL:(NSURL *)dataStoreURL toDestinationModel:(NSManagedObjectModel *)destinationModel
{
    NSString *type = NSSQLiteStoreType;
    NSDictionary *sourceStoreMetadata = [NSPersistentStoreCoordinator metadataForPersistentStoreOfType:type
                                                                                                   URL:dataStoreURL
                                                                                                 error:NULL];

    if ( !sourceStoreMetadata )
    {
        return NO;
    }

    int destinationVersion = [[[destinationModel versionIdentifiers] anyObject] intValue];
    int sourceVersion = [[sourceStoreMetadata[NSStoreModelVersionIdentifiersKey] lastObject] intValue];
    return ![destinationModel isConfiguration:nil compatibleWithStoreMetadata:sourceStoreMetadata] || destinationVersion != sourceVersion;
}

- (BOOL)mergeJournalToDataStoreAtURL:(NSURL *)dataStoreURL
{
    NSDictionary *sourceStoreMetadata = [NSPersistentStoreCoordinator metadataForPersistentStoreOfType:NSSQLiteStoreType
                                                                                                   URL:dataStoreURL
                                                                                                 error:NULL];
    if ( !sourceStoreMetadata )
    {
        return NO;
    }

    NSManagedObjectModel *sourceModel = [NSManagedObjectModel mergedModelFromBundles:@[self.bundle]
                                                                    forStoreMetadata:sourceStoreMetadata];
    if ( !sourceModel )
    {
        return NO;
    }

    NSPersistentStoreCoordinator *persistentStoreCoordinator = [[NSPersistentStoreCoordinator alloc] initWithManagedObjectModel:sourceModel];
    NSPersistentStore *persistentStore = [persistentStoreCoordinator addPersistentStoreWithType:NSSQLiteStoreType
                                                                                  configuration:nil
                                                                                            URL:dataStoreURL
                                                                                        options:@{NSSQLitePragmasOption : @{@"journal_mode" : @"delete"}}
                                                                                          error:NULL];
    return persistentStore != nil;
}

+ (instancetype)sharedInstance
{
    @synchronized(self) {
        static NSMutableDictionary *_sharedDataStoreManagers = nil;
        static dispatch_once_t onceToken;
        dispatch_once(&onceToken, ^{
            _sharedDataStoreManagers = [NSMutableDictionary dictionary];
        });

        NSString *uniqueKey = NSStringFromClass(self.class);
        SLCoreDataStack *instance = _sharedDataStoreManagers[uniqueKey];

        if (!instance) {
            instance = [[super allocWithZone:NULL] init];
            _sharedDataStoreManagers[uniqueKey] = instance;
        }

        return instance;
    }
}

#pragma mark - Memory management

- (instancetype)init
{
    if ( self = [super init] )
    {
        self.pendingDeletedObjects = [NSMutableArray array];
    }
    return self;
}

- (void)dealloc
{
    [[NSNotificationCenter defaultCenter] removeObserver:self];
}

#pragma mark - Class methods

+ (BOOL)subclassesRequireMigration
{
    __block BOOL subclassesRequireMigration = NO;

    for (NSString *className in [self _concreteSubclasses]) {
        Class class = NSClassFromString(className);

        SLCoreDataStack *manager = [class sharedInstance];
        if (manager.requiresMigration) {
            subclassesRequireMigration = YES;
        }
    }

    return subclassesRequireMigration;
}

+ (void)registerConcreteSubclass:(Class)subclass
{
    NSParameterAssert(subclass);
    NSAssert([subclass isSubclassOfClass:[SLCoreDataStack class]], @"%@ needs to be a concrete subclass of SLCoreDataStack", subclass);
    NSAssert(subclass != [SLCoreDataStack class], @"%@ needs to be a concrete subclass of SLCoreDataStack", subclass);

    [[self _concreteSubclasses] addObject:NSStringFromClass(subclass)];
}

+ (NSMutableSet *)_concreteSubclasses
{
    static NSMutableSet *set = nil;

    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        set = [NSMutableSet set];
    });

    return set;
}

+ (void)migrateSubclassesWithProgressHandler:(void(^)(SLCoreDataStack *currentMigratingSubclass))progressHandler
                           completionHandler:(dispatch_block_t)completionHandler
{
    static dispatch_queue_t queue = NULL;

    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        queue = dispatch_queue_create("de.ebf.SLCoreDataStack.migration-queue", DISPATCH_QUEUE_CONCURRENT);
    });

    NSMutableArray *requiresSubclasses = [NSMutableArray array];

    for (NSString *className in [self _concreteSubclasses]) {
        Class class = NSClassFromString(className);

        SLCoreDataStack *manager = [class sharedInstance];
        if (manager.requiresMigration) {
            [requiresSubclasses addObject:manager];
        }
    }

    NSUInteger count = requiresSubclasses.count;
    [requiresSubclasses enumerateObjectsUsingBlock:^(SLCoreDataStack *manager, NSUInteger idx, BOOL *stop) {
        dispatch_async(queue, ^{
            if (progressHandler) {
                dispatch_async(dispatch_get_main_queue(), ^{
                    progressHandler(manager);
                });
            }

            // automatically triggers migration if available
            [manager mainThreadManagedObjectContext];

            if (idx + 1 == count) {
                if (completionHandler) {
                    dispatch_async(dispatch_get_main_queue(), ^{
                        completionHandler();
                    });
                }
            }
        });
    }];
}

#pragma mark - CoreData

- (NSManagedObjectModel *)managedObjectModel
{
    if (!_managedObjectModel) {
        NSString *managedObjectModelName = self.managedObjectModelName;
        NSURL *modelURL = [self.bundle URLForResource:managedObjectModelName withExtension:@"momd"];

        _managedObjectModel = [[NSManagedObjectModel alloc] initWithContentsOfURL:modelURL];
    }

    return _managedObjectModel;
}

- (NSManagedObjectContext *)mainThreadManagedObjectContext
{
    if (!_mainThreadManagedObjectContext) {
        _mainThreadManagedObjectContext = [[NSManagedObjectContext alloc] initWithConcurrencyType:NSMainQueueConcurrencyType];
        _mainThreadManagedObjectContext.persistentStoreCoordinator = self.persistentStoreCoordinator;
        _mainThreadManagedObjectContext.mergePolicy = self.mainThreadMergePolicy;
        _mainThreadManagedObjectContext.undoManager = nil;
    }

    return _mainThreadManagedObjectContext;
}

- (void)setMainThreadManagedObjectContext:(NSManagedObjectContext *)mainThreadManagedObjectContext
{
    if (mainThreadManagedObjectContext != _mainThreadManagedObjectContext) {
        _mainThreadManagedObjectContext = mainThreadManagedObjectContext;
    }
}

- (NSManagedObjectContext *)backgroundThreadManagedObjectContext
{
    if (!_backgroundThreadManagedObjectContext) {
        _backgroundThreadManagedObjectContext = [[NSManagedObjectContext alloc] initWithConcurrencyType:NSPrivateQueueConcurrencyType];
        _backgroundThreadManagedObjectContext.persistentStoreCoordinator = self.persistentStoreCoordinator;
        _backgroundThreadManagedObjectContext.mergePolicy = self.backgroundThreadMergePolicy;
        _backgroundThreadManagedObjectContext.undoManager = nil;

        [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(_managedObjectContextWillSaveNotificationCallback:) name:NSManagedObjectContextWillSaveNotification object:_backgroundThreadManagedObjectContext];
        [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(_managedObjectContextDidSaveNotificationCallback:) name:NSManagedObjectContextDidSaveNotification object:_backgroundThreadManagedObjectContext];
    }

    return _backgroundThreadManagedObjectContext;
}

- (void)setBackgroundThreadManagedObjectContext:(NSManagedObjectContext *)backgroundThreadManagedObjectContext
{
    if (backgroundThreadManagedObjectContext != _backgroundThreadManagedObjectContext) {
        [[NSNotificationCenter defaultCenter] removeObserver:self name:NSManagedObjectContextWillSaveNotification object:_backgroundThreadManagedObjectContext];
        [[NSNotificationCenter defaultCenter] removeObserver:self name:NSManagedObjectContextDidSaveNotification object:_backgroundThreadManagedObjectContext];

        _backgroundThreadManagedObjectContext = backgroundThreadManagedObjectContext;

        [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(_managedObjectContextWillSaveNotificationCallback:) name:NSManagedObjectContextWillSaveNotification object:_backgroundThreadManagedObjectContext];
        [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(_managedObjectContextDidSaveNotificationCallback:) name:NSManagedObjectContextDidSaveNotification object:_backgroundThreadManagedObjectContext];
    }
}

- (NSPersistentStoreCoordinator *)persistentStoreCoordinator
{
    if (!_persistentStoreCoordinator) {
        NSURL *storeURL = self.dataStoreURL;
        NSManagedObjectModel *managedObjectModel = self.managedObjectModel;

        NSError *error = nil;
        _persistentStoreCoordinator = [[NSPersistentStoreCoordinator alloc] initWithManagedObjectModel:managedObjectModel];
        if ( [self requiresMigrationFromDataStoreAtURL:storeURL toDestinationModel:managedObjectModel] )
        {
            [self mergeJournalToDataStoreAtURL:storeURL];

            if ( ![self _performMigrationFromDataStoreAtURL:storeURL toDestinationModel:managedObjectModel error:&error] )
            {
                [[NSFileManager defaultManager] removeItemAtURL:storeURL error:NULL];
            }
        }

        if ( ![_persistentStoreCoordinator addPersistentStoreWithType:NSSQLiteStoreType configuration:nil URL:storeURL options:nil error:&error] )
        {
            NSAssert(NO, @"Could not add persistent store: %@", error);
        }
    }

    return _persistentStoreCoordinator;
}

- (void)removePersistentStoreCoordinator
{
    [_mainThreadManagedObjectContext lock];
    [_mainThreadManagedObjectContext reset];

    [_backgroundThreadManagedObjectContext lock];
    [_backgroundThreadManagedObjectContext reset];

    for (NSPersistentStore *store in [_persistentStoreCoordinator persistentStores])
    {
        NSError *error = nil;
        if ( ![_persistentStoreCoordinator removePersistentStore:store error:&error] )
        {
            NSAssert(NO, @"Could not remove persistent store: %@", error);
            return;
        }
    }

    [_backgroundThreadManagedObjectContext unlock];
    [_mainThreadManagedObjectContext unlock];

    _persistentStoreCoordinator = nil;

    _backgroundThreadManagedObjectContext = nil;
    _mainThreadManagedObjectContext = nil;
}

#pragma mark - private implementation ()

- (BOOL)_performMigrationFromDataStoreAtURL:(NSURL *)dataStoreURL
                         toDestinationModel:(NSManagedObjectModel *)destinationModel
                                      error:(NSError **)error
{
    NSAssert(error != nil, @"Error pointer cannot be nil");

    NSString *type = NSSQLiteStoreType;
    NSDictionary *sourceStoreMetadata = [NSPersistentStoreCoordinator metadataForPersistentStoreOfType:type
                                                                                                   URL:dataStoreURL
                                                                                                 error:error];

    if (!sourceStoreMetadata) {
        return NO;
    }

    int destinationVersion = [[[destinationModel versionIdentifiers] anyObject] intValue];
    int sourceVersion = [[sourceStoreMetadata[NSStoreModelVersionIdentifiersKey] lastObject] intValue];
    if ( destinationVersion == sourceVersion && [destinationModel isConfiguration:nil compatibleWithStoreMetadata:sourceStoreMetadata] ) {
        *error = nil;
        return YES;
    }

    NSArray *bundles = @[ self.bundle ];
    NSManagedObjectModel *sourceModel = [NSManagedObjectModel mergedModelFromBundles:bundles
                                                                    forStoreMetadata:sourceStoreMetadata];

    if (!sourceModel) {
        NSDictionary *userInfo = [NSDictionary dictionaryWithObject:[NSString stringWithFormat:@"Unable to find NSManagedObjectModel for store metadata %@", sourceStoreMetadata]
                                                             forKey:NSLocalizedDescriptionKey];
        *error = [NSError errorWithDomain:SLCoreDataStackErrorDomain code:SLCoreDataStackManagedObjectModelNotFound userInfo:userInfo];
        return NO;
    }

    NSMutableArray *objectModelPaths = [NSMutableArray array];
    NSArray *allManagedObjectModels = [self.bundle pathsForResourcesOfType:@"momd"
                                                               inDirectory:nil];

    for (NSString *managedObjectModelPath in allManagedObjectModels) {
        NSArray *array = [self.bundle pathsForResourcesOfType:@"mom"
                                                  inDirectory:managedObjectModelPath.lastPathComponent];

        [objectModelPaths addObjectsFromArray:array];
    }

    NSArray *otherModels = [self.bundle pathsForResourcesOfType:@"mom" inDirectory:nil];
    [objectModelPaths addObjectsFromArray:otherModels];

    if (objectModelPaths.count == 0) {
        NSDictionary *userInfo = [NSDictionary dictionaryWithObject:[NSString stringWithFormat:@"No NSManagedObjectModel found in bundle %@", self.bundle]
                                                             forKey:NSLocalizedDescriptionKey];
        *error = [NSError errorWithDomain:SLCoreDataStackErrorDomain code:SLCoreDataStackManagedObjectModelNotFound userInfo:userInfo];
        return NO;
    }

    NSMappingModel *mappingModel = nil;
    NSManagedObjectModel *targetModel = nil;
    NSString *modelPath = nil;

    int targetVersion;
    for (modelPath in objectModelPaths) {
        NSURL *modelURL = [NSURL fileURLWithPath:modelPath];
        targetModel = [[NSManagedObjectModel alloc] initWithContentsOfURL:modelURL];

        targetVersion = [[[targetModel versionIdentifiers] anyObject] intValue];
        if ( targetVersion != sourceVersion + 1 )
        {
            continue;
        }

        mappingModel = [NSMappingModel mappingModelFromBundles:bundles
                                                forSourceModel:sourceModel
                                              destinationModel:targetModel];

        if ( !mappingModel )
        {
            mappingModel = [NSMappingModel inferredMappingModelForSourceModel:sourceModel
                                                             destinationModel:targetModel
                                                                        error:NULL];
        }

        if ( mappingModel )
        {
            break;
        }
    }

    if (!mappingModel) {
        NSDictionary *userInfo = [NSDictionary dictionaryWithObject:[NSString stringWithFormat:@"Unable to find NSMappingModel for store at URL %@", dataStoreURL]
                                                             forKey:NSLocalizedDescriptionKey];
        *error = [NSError errorWithDomain:SLCoreDataStackErrorDomain code:SLCoreDataStackMappingModelNotFound userInfo:userInfo];
        return NO;
    }

    [[NSNotificationCenter defaultCenter] postNotificationName:SLCoreDataStackWillPerformMigrationStep
                                                        object:self
                                                      userInfo:@{SLSourceModelVersionKey: @(sourceVersion),
                                                                 SLTargetModelVersionKey: @(targetVersion)}];

    NSMigrationManager *migrationManager = [[NSMigrationManager alloc] initWithSourceModel:sourceModel
                                                                          destinationModel:targetModel];

    NSString *modelName = modelPath.lastPathComponent.stringByDeletingPathExtension;
    NSString *storeExtension = dataStoreURL.path.pathExtension;

    NSString *storePath = dataStoreURL.path.stringByDeletingPathExtension;

    NSString *destinationPath = [NSString stringWithFormat:@"%@.%@.%@", storePath, modelName, storeExtension];
    NSURL *destinationURL = [NSURL fileURLWithPath:destinationPath];

    [[NSFileManager defaultManager] removeItemAtURL:destinationURL error:NULL];

    if (![migrationManager migrateStoreFromURL:dataStoreURL type:type options:nil withMappingModel:mappingModel toDestinationURL:destinationURL destinationType:type destinationOptions:nil error:error]) {
        return NO;
    }

    [[NSNotificationCenter defaultCenter] postNotificationName:SLCoreDataStackDidPerformMigrationStep
                                                        object:self
                                                      userInfo:@{SLTemporaryDataStoreURL: destinationURL,
                                                                 SLSourceModelVersionKey: @(sourceVersion),
                                                                 SLTargetModelVersionKey: @(targetVersion)}];

    // atomically replace main store with successfully migrated store
    if ( 0 != rename([[destinationURL path] cStringUsingEncoding:NSUTF8StringEncoding], [[dataStoreURL path] cStringUsingEncoding:NSUTF8StringEncoding]) )
    {
        return NO;
    }

    return [self _performMigrationFromDataStoreAtURL:dataStoreURL
                                  toDestinationModel:destinationModel
                                               error:error];
}

- (void)_managedObjectContextWillSaveNotificationCallback:(NSNotification *)notification
{
    NSManagedObjectContext *changedContext = notification.object;
    NSManagedObjectContext *otherContext = self.mainThreadManagedObjectContext;
    if ( changedContext == self.backgroundThreadManagedObjectContext )
    {
        NSMutableSet *deletedObjectIDs = [NSMutableSet set];
        for (NSManagedObject *object in changedContext.deletedObjects)
        {
            [deletedObjectIDs addObject:object.objectID];
        }

        // Explicitly unfault and save (retain) all deleted objects until context did save.
        // Fix NSObjectInaccessibleException when deleted object is fault in other context.
        if ( [deletedObjectIDs count] )
        {
            [otherContext performBlockAndWait:^{
                for (NSManagedObjectID *objectID in deletedObjectIDs)
                {
                    NSManagedObject *deletedObject = [otherContext existingObjectWithID:objectID error:NULL];
                    if ( deletedObject )
                    {
                        [self.pendingDeletedObjects addObject:deletedObject];
                    }
                }
            }];
        }
    }
}

- (void)_managedObjectContextDidSaveNotificationCallback:(NSNotification *)notification
{
    NSManagedObjectContext *changedContext = notification.object;
    NSManagedObjectContext *otherContext = self.mainThreadManagedObjectContext;
    if ( changedContext == self.backgroundThreadManagedObjectContext )
    {
        [otherContext performBlockAndWait:^{
            // Explicitly unfault all updated objects.
            NSArray *updatedObjects = [[notification.userInfo objectForKey:@"updated"] allObjects];
            for (NSInteger index = 0; index < [updatedObjects count]; index++)
            {
                NSManagedObjectID *objectID = [updatedObjects[index] objectID];
                [otherContext existingObjectWithID:objectID error:NULL];
            }

            // Merge notification.
            [otherContext mergeChangesFromContextDidSaveNotification:notification];
            [[NSNotificationCenter defaultCenter] postNotificationName:SLCoreDataStackDidMergeChangesNotification object:nil];

            // Don't retain deleted objects.
            [self.pendingDeletedObjects removeAllObjects];
        }];
    }
}

@end
