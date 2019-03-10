package repair

// //WalkPair holds the result of a walk entry
// type WalkPair [2]string

// //Key return the walk entry key
// func (w WalkPair) Key() string { return w[0] }

// //Path return the walk entry path
// func (w WalkPair) Path() string { return w[1] }

// func walkBucket(ctx context.Context, dir, bucket string) chan WalkPair {
// 	ch := make(chan WalkPair)
// 	trim := filepath.Join(dir, "objects") + "/"
// 	base := filepath.Join(trim, bucket)

// 	go func() {
// 		defer close(ch)
// 		filepath.Walk(base, func(path string, info os.FileInfo, err error) error {
// 			if err != nil {
// 				//log and continue to process remaining files
// 				log.WithField("path", path).WithError(err).Error("failed to walk")
// 				return nil
// 			}

// 			if info.IsDir() {
// 				return nil
// 			}
// 			key := strings.TrimPrefix(path, trim)
// 			select {
// 			case ch <- WalkPair{key, path}:
// 			case <-ctx.Done():
// 				return ctx.Err()
// 			}

// 			return nil
// 		})
// 	}()

// 	return ch
// }

// func loadMeta(marshaler *encoding.MarshalFuncPair, path string) (*metatypes.Metadata, error) {
// 	bytes, err := ioutil.ReadFile(path)
// 	if err != nil {
// 		return nil, err
// 	}

// 	var m metatypes.Metadata
// 	return &m, marshaler.Unmarshal(bytes, &m)
// }
