package net.adamsmolnik.handler;

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
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;

import net.adamsmolnik.handler.exception.UploadPhotoHandlerException;
import net.adamsmolnik.handler.model.ImageMetadata;
import net.adamsmolnik.handler.util.ImageMetadataExplorer;
import net.adamsmolnik.handler.util.ImageResizer;
import net.adamsmolnik.handler.util.ResizerResult;

/**
 * @author asmolnik
 *
 */
public class UploadPhotoHandler {

	private static final String DEST_BUCKET = "smolnik.photos";

	private static final String KEY_PREFIX = "photos/";

	private static final String ARCH_BUCKET = "arch." + DEST_BUCKET;

	private static final String JPEG_EXT = "jpg";

	private static final DateTimeFormatter DT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-A");

	private static final int THUMBNAIL_SIZE = 300;

	private static final int WEB_IMAGE_SIZE = 1080;

	private static final ThreadLocal<AmazonS3> thlS3 = new ThreadLocal<AmazonS3>() {

		@Override
		protected AmazonS3 initialValue() {
			return new AmazonS3Client();
		}

	};

	private static final ThreadLocal<AmazonDynamoDB> thlDb = new ThreadLocal<AmazonDynamoDB>() {

		@Override
		protected AmazonDynamoDB initialValue() {
			return new AmazonDynamoDBClient();
		}

	};

	public void handle(S3Event s3Event, Context context) {
		LambdaLogger log = context.getLogger();
		ExecutorService es = Executors.newCachedThreadPool();
		try {
			s3Event.getRecords().forEach(record -> {
				try {
					process(new S3ObjectStream(record.getS3(), record.getUserIdentity()), log, es);
				} catch (IOException e) {
					throw new UploadPhotoHandlerException(e);
				}
			});
		} finally {
			es.shutdownNow();
		}

	}

	private void process(S3ObjectStream os, LambdaLogger log, ExecutorService es) throws IOException {
		final AmazonS3 s3 = thlS3.get();
		String srcBucket = os.getBucket();
		String srcKey = os.getKey();
		log.log("File uploaded: " + os.getKey());
		MediaType mediaType = detect(os.newCachedInputStream());
		if (!"jpeg".equals(mediaType.getSubtype())) {
			s3.copyObject(srcBucket, srcKey, "upload-unidentified", srcKey);
			return;
		}
		ImageMetadata imd = new ImageMetadataExplorer().explore(os.newCachedInputStream());
		ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(imd.getPhotoTaken().getTime()), ZoneId.of("UTC"));
		String userId = os.getUserId();
		String userKeyPrefix = KEY_PREFIX + userId + "/";
		String baseDestKey = createDestKey(srcKey, zdt, JPEG_EXT);
		String photoKey = userKeyPrefix + baseDestKey;
		String thumbnailKey = userKeyPrefix + "thumbnails/" + baseDestKey;

		List<Future<?>> futures = Arrays.<Future<?>> asList(es.submit(() -> {
			s3.copyObject(srcBucket, srcKey, ARCH_BUCKET, photoKey);
		}), es.submit(() -> {
			putS3Object(thumbnailKey, new ImageResizer(os.newCachedInputStream(), THUMBNAIL_SIZE).resize());
		}), es.submit(() -> {
			putS3Object(photoKey, new ImageResizer(os.newCachedInputStream(), WEB_IMAGE_SIZE).resize());
		}));
		await(futures);
		PutRequest pr = new PutRequest().withUserId(userId).withPrincipalId(os.getPrincipalId()).withPhotoKey(photoKey).withThumbnailKey(thumbnailKey)
				.withZonedDateTime(zdt).withImageMetadata(imd);
		thlDb.get().putItem(createPutRequest(pr));
	}

	private void putS3Object(String objectKey, ResizerResult rr) {
		AmazonS3 s3 = new AmazonS3Client();
		ObjectMetadata md = new ObjectMetadata();
		md.setContentLength(rr.getSize());
		s3.putObject(DEST_BUCKET, objectKey, rr.getInputStream(), md);
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

	private String createDestKey(String srcKey, ZonedDateTime zdt, String ext) {
		return zdt.format(DT_FORMATTER) + "-" + Integer.toHexString(UUID.randomUUID().toString().hashCode()) + "." + ext;
	}

	private static MediaType detect(InputStream inputStream) {
		try (InputStream is = inputStream) {
			DefaultDetector dd = new DefaultDetector();
			return dd.detect(TikaInputStream.get(is), new Metadata());
		} catch (IOException e) {
			throw new UploadPhotoHandlerException(e);
		}
	}

}
