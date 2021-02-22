header中新增topic=odeon_test_%{collectionName}，%{collectionName}代表由header中获取的collectionName的value
```
interceptors=header_extractor
interceptors.header_extractor.type=org.apache.flume.interceptor.HeaderExtractorInterceptor$Builder
interceptors.header_extractor.key=topic
interceptors.header_extractor.value=odeon_test_%{collectionName}
```