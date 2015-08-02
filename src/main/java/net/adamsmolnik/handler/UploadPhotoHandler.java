package net.adamsmolnik.handler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3BucketEntity;
import com.amazonaws.services.s3.event.S3EventNotification.S3Entity;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.event.S3EventNotification.S3ObjectEntity;

import net.adamsmolnik.handler.exception.UploadPhotoHandlerException;
import net.adamsmolnik.handler.model.ImageMetadata;
import net.adamsmolnik.util.ImageMetadataExplorer;
import net.adamsmolnik.util.ImageResizer;

/**
 * @author asmolnik
 *
 */
public class UploadPhotoHandler {

	private static class PutRequest {

		private String userId;

		private String principalId;

		private String photoKey;

		private String thumbnailKey;

		private ZonedDateTime zdt;

		private ImageMetadata imd;

		private PutRequest withUserId(String userId) {
			this.userId = userId;
			return this;
		}

		private PutRequest withPrincipalId(String principalId) {
			this.principalId = principalId;
			return this;
		}

		private PutRequest withPhotoKey(String photoKey) {
			this.photoKey = photoKey;
			return this;
		}

		private PutRequest withThumbnailKey(String thumbnailKey) {
			this.thumbnailKey = thumbnailKey;
			return this;
		}

		private PutRequest withZonedDateTime(ZonedDateTime zdt) {
			this.zdt = zdt;
			return this;
		}

		private PutRequest withImageMetadata(ImageMetadata imd) {
			this.imd = imd;
			return this;
		}

	}

	private static final String DEST_BUCKET = "smolnik.photos";

	private static final String KEY_PREFIX = "photos/";

	private static final String ARCH_BUCKET = "arch." + DEST_BUCKET;

	private static final String JPEG_EXT = "jpg";

	private final AmazonS3 s3 = new AmazonS3Client();

	private final AmazonDynamoDB db = new AmazonDynamoDBClient();

	public void handle(S3Event s3Event, Context context) {
		LambdaLogger logger = context.getLogger();
		ExecutorService es = Executors.newCachedThreadPool();
		try {
			s3Event.getRecords().forEach(record -> {
				try {
					process(new S3ObjectStream(record.getS3(), record.getUserIdentity()), logger, es);
				} catch (IOException e) {
					throw new UploadPhotoHandlerException(e);
				}
			});
		} finally {
			es.shutdownNow();
		}

	}

	private void process(S3ObjectStream os, LambdaLogger logger, ExecutorService es) throws IOException {
		String srcBucket = os.getBucket();
		String srcKey = os.getKey();
		logger.log("File uploaded: " + os.getKey());
		MediaType mediaType = detect(os.newCachedInputStream());
		if ("jpeg".equals(mediaType.getSubtype())) {
			ImageMetadata imd = new ImageMetadataExplorer().explore(os.newCachedInputStream());
			ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(imd.getPhotoTaken().getTime()), ZoneId.of("UTC"));
			String userId = os.getUserId();
			String userKeyPrefix = KEY_PREFIX + userId + "/";
			String baseDestKey = createDestKey(srcKey, zdt, UUID.randomUUID().toString(), JPEG_EXT);
			String photoKey = userKeyPrefix + baseDestKey;
			String thumbnailKey = userKeyPrefix + "thumbnails/" + baseDestKey;

			List<Future<?>> futures = Arrays.<Future<?>> asList(es.submit(() -> {
				s3.copyObject(srcBucket, srcKey, ARCH_BUCKET, photoKey);
			}), es.submit(() -> {
				s3.putObject(DEST_BUCKET, thumbnailKey, new ImageResizer(os.newCachedInputStream(), 300).resize());
			}), es.submit(() -> {
				s3.putObject(DEST_BUCKET, photoKey, new ImageResizer(os.newCachedInputStream(), 1080).resize());
			}));
			await(futures);
			PutRequest pr = new PutRequest().withUserId(userId).withPrincipalId(os.getPrincipalId()).withPhotoKey(photoKey)
					.withThumbnailKey(thumbnailKey).withZonedDateTime(zdt).withImageMetadata(imd);
			db.putItem(createPutRequest(pr));
		} else {
			s3.copyObject(srcBucket, srcKey, "upload-unidentified", srcKey);
		}
		s3.deleteObject(srcBucket, srcKey);
	}

	private PutItemRequest createPutRequest(PutRequest pr) {
		return new PutItemRequest().withTableName("photos").addItemEntry("userId", new AttributeValue(pr.userId))
				.addItemEntry("photoTakenDate", new AttributeValue(pr.zdt.format(DateTimeFormatter.ISO_LOCAL_DATE)))
				.addItemEntry("photoTakenTime", new AttributeValue(pr.zdt.format(DateTimeFormatter.ISO_LOCAL_TIME)))
				.addItemEntry("photoKey", new AttributeValue(pr.photoKey)).addItemEntry("thumbnailKey", new AttributeValue(pr.thumbnailKey))
				.addItemEntry("bucket", new AttributeValue(DEST_BUCKET)).addItemEntry("madeBy", new AttributeValue(pr.imd.getMadeBy()))
				.addItemEntry("model", new AttributeValue(pr.imd.getModel())).addItemEntry("principalId", new AttributeValue(pr.principalId));
	}

	private void await(List<Future<?>> futures) {
		List<Exception> exceptions = new ArrayList<>();
		futures.forEach(f -> {
			try {
				f.get();
			} catch (Exception e) {
				exceptions.add(e);
			}
		});
		if (!exceptions.isEmpty()) {
			throw new UploadPhotoHandlerException(exceptions);
		}
	}

	private String createDestKey(String srcKey, ZonedDateTime zdt, String uuid, String ext) {
		return zdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-A")) + "-" + uuid.hashCode() + "." + ext;
	}

	private static MediaType detect(InputStream inputStream) {
		try (InputStream is = inputStream) {
			DefaultDetector dd = new DefaultDetector();
			return dd.detect(TikaInputStream.get(is), new Metadata());
		} catch (IOException e) {
			throw new UploadPhotoHandlerException(e);
		}
	}

	public static void main(String[] args) throws Exception {
		File file = new File("/photos/hiszpania2015/20150616_124937.jpg");
		S3ObjectEntity s3ObjectEntity = new S3ObjectEntity(file.getName(), file.length(), null, null);
		S3Entity s3Entity = new S3Entity(null, new S3BucketEntity("upload-photos-ext", null, null), s3ObjectEntity, null);
		S3EventNotificationRecord record = new S3EventNotificationRecord(null, null, null, null, null, null, null, s3Entity, null);
		S3Event s3Event = new S3Event(Arrays.asList(record));
		new UploadPhotoHandler().handle(s3Event, new Context() {

			@Override
			public int getRemainingTimeInMillis() {
				return 0;
			}

			@Override
			public int getMemoryLimitInMB() {
				return 0;
			}

			@Override
			public LambdaLogger getLogger() {
				return new LambdaLogger() {

					@Override
					public void log(String s) {
						System.out.println(s);

					}
				};
			}

			@Override
			public String getLogStreamName() {
				return null;
			}

			@Override
			public String getLogGroupName() {
				return null;
			}

			@Override
			public CognitoIdentity getIdentity() {
				return null;
			}

			@Override
			public String getFunctionName() {
				return null;
			}

			@Override
			public ClientContext getClientContext() {
				return null;
			}

			@Override
			public String getAwsRequestId() {
				return null;
			}
		});
	}

}
