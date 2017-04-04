import static com.google.common.collect.Lists.transform;
import static java.nio.charset.StandardCharsets.UTF_8;
import static rx.Observable.from;
import static rx.Observable.merge;



public Observable<String> watch(String path) {
    return Observable.create(observer -> {
        AtomicBoolean unsubscribed = new AtomicBoolean(false);
        
        BackgroundCallback callback = (client, event) -> {
            switch (Code.get(event.getResultCode())) {
                case OK:
                    byte[] bytes = event.getData();
                    if (bytes != null) {
                        observer.onNext(new String(bytes, UTF_8));
                    }
                    break;
                case NONODE:
                    observer.onError(new NoNodeException(path));
            }
        };
        
        Consumer<Watcher> watch = watcher -> {
            try {
                curator.getData().usingWatcher(watcher).inBackground(callback).forPath(path);
            } catch (Exception e) {
                observer.onError(e);
            }
        };
        
        watch.accept(new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (unsubscribed.get()) return;
                
                switch (event.getType()) {
                    case NodeCreated:
                    case NodeDataChanged:
                        watch.accept(this);
                    case NodeDeleted:
                        observer.onCompleted();
                }
            }
        });
        
        return Subscriptions.create(() -> unsubscribed.set(true));
    });
}

public Observable<List<String>> watchChildren(String path) {
    return Observable.create(observer -> {
        AtomicBoolean unsubscribed = new AtomicBoolean(false);
        
        BackgroundCallback callback = (client, event) -> {
            switch (Code.get(event.getResultCode()) {
                case OK:
                    List<String> children = event.getChildren();
                    observer.onNext(children != null ? children : Collections.emptyList());
                    break;
                case NONODE:
                    observer.onError(new NoNodeException(path));
            }
        };
        
        Consumer<Watcher> watch = watcher -> {
            try {
                curator.getChildren().usingWatcher(watcher).inBackground(callback).forPath(path);
            } catch (Exception e) {
                observer.onError(e);
            }
        };
        
        watch.accept(new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (unsubscribed.get()) return;
                
                switch (event.getType()) {
                    case NodeCreated:
                    case NodeChildrenChanged:
                        watch.accept(this);
                        break;
                    case NodeDeleted:
                        observer.onCompleted();
                }
            }
        });
        
        return Subscriptions.create(() -> unsubscribed.set(true));
    });
}

public Observable<List<String>> getChildren(String path) {
    return Observable.create(observer -> {
        try {
            curator.getChildren().inBackground((client, event) -> {
                Code code = Code.get(event.getResultCode());
                switch (code) {
                    case OK:
                        observer.onNext(event.getChildren());
                        observer.onCompleted();
                        break;
                    default:
                        observer.onError(KeeperException.create(code));
                }
            }).forPath(path);
        } catch (Exception e) {
            observer.onError(e);
        }
        
        return Subscriptions.empty();
    });
}

public Observable<Void> delete(String path) {
    return Observable.create(observer -> {
        try {
            curator.delete().inBackground((client, event) -> {
                Code code = Code.get(event.getResultCode());
                switch (code) {
                    case OK:
                    case NONODE:
                        observer.onCompleted();
                        break;
                    default:
                        observer.onError(KeeperException.create(code));
                }
            }).forPath(path);
        } catch (Exception e) {
            observer.onError(e);
        }
        
        return Subscriptions.empty();
    });
}

public Observable<Void> deleteAll(String path) {
    return getChildren(path).flatMap(children -> {
        List<Observable<Void>> requests = transform(children, child -> 
            deleteAll(path + "/" + child)
        );
        return merge(from(requests)).lastOrDefault(null);
    }).flatMap(done -> delete(path));
}
