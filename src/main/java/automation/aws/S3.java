package automation.aws;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.StringInputStream;
import org.apache.commons.io.IOUtils;

public class S3 {
    private String bucketName;
    private AmazonS3 s3client;
    public static Regions region = Regions.US_EAST_1;
    public static  int urlExpiration = 360;
    private static Logger logger;

    static
    {
        try {
            if(System.getProperty("java.util.logging.config.file") == null){
                String file = S3.class.getResource("/logging.properties").getFile();
                System.setProperty("java.util.logging.config.file", file);
            }
            logger = Logger.getLogger("qa.automation.aws.S3");

        }catch (Exception ex){
            logger.severe(ex.getMessage());
        }
    }

    public enum FileExtensions
    {
        html

    }

    private void logMessage(AmazonClientException ex){
        logger.severe(ex.getMessage());
    }

    private void logMessage(AmazonServiceException ex){
        if(ex.getStatusCode() == 404){
            logger.info(ex.getMessage());
        } else {
            logger.severe(ex.getMessage());
        }

    }

    public S3(String key,String secret,String bucketName){
        logger.info("initializing s3 client");
        AWSCredentials credentials = new BasicAWSCredentials(key,secret);
        s3client = AmazonS3ClientBuilder.standard().withRegion(region).withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
        this.bucketName = bucketName;
        logger.info("s3 client initialized successfully");
    }

    public S3(AmazonS3ClientBuilder amazonS3ClientBuilder)
    {
        logger.info("initializing s3 client");
        s3client = amazonS3ClientBuilder.build();
        logger.info("s3 client initialized successfully");
    }

    public boolean createBucket(String bucketName)
    {
        try {
            s3client.createBucket(bucketName);
            return true;

        } catch (AmazonServiceException ase) {
            logMessage(ase);
            return false;
        } catch (AmazonClientException ace) {
            logMessage(ace);
            return false;
        }
    }

    public boolean createFolder(String folderName)
    {
        try {
            if(!folderName.endsWith(File.separator))
            {
                folderName += File.separator;
            }
            s3client.putObject(bucketName, folderName, "");
            return true;

        } catch (AmazonServiceException ase) {
            logMessage(ase);
            return false;
        } catch (AmazonClientException ace) {
            logMessage(ace);
            return false;
        }
    }

    public boolean deleteFile(String file)
    {
        try {
            s3client.deleteObject(bucketName, file);
            return true;

        } catch (AmazonServiceException ase) {
            logMessage(ase);
            return false;
        } catch (AmazonClientException ace) {
            logMessage(ace);
            return false;
        }
    }

    public boolean uploadFile(String fileName,String content)
    {
        try {
            s3client.putObject(bucketName, fileName, content);
            return true;
        } catch (AmazonServiceException ase) {
            logMessage(ase);
            return false;
        } catch (AmazonClientException ace) {
            logMessage(ace);
            return false;
        }
    }

    public boolean uploadFile(String fileName,String content,FileExtensions fileExtensions) throws Exception
    {
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            switch (fileExtensions.toString())
            {
                case "html":
                    metadata.setContentType("application/html");
                    metadata.setContentDisposition("attachment; filename=\""+fileName+"\"");
                    break;
                default:metadata.setContentType("text");
                    break;
            }
            byte[] b = content.getBytes();
            metadata.setContentLength(b.length);
            s3client.putObject(bucketName,fileName, new StringInputStream(content),metadata);
            return true;
        } catch (AmazonServiceException ase) {
            logMessage(ase);
            return false;
        } catch (AmazonClientException ace) {
            logMessage(ace);
            return false;
        }
    }

    public boolean uploadFile(String path,File file)
    {
        try {
            s3client.putObject(bucketName,path,file);
            return true;
        } catch (AmazonServiceException ase) {
            logMessage(ase);
            return false;
        } catch (AmazonClientException ace) {
            logMessage(ace);
            return false;
        }
    }

    public boolean uploadFile(String path, InputStream stream)
    {
        try {
            s3client.putObject(bucketName,path,stream,null);
            return true;
        } catch (AmazonServiceException ase) {
            logMessage(ase);
            return false;
        } catch (AmazonClientException ace) {
            logMessage(ace);
            return false;
        }
    }

    public String getFile(String file) throws Exception
    {
        String data = "";
        try {
            S3Object s3Object = s3client.getObject(bucketName, file);
            data = IOUtils.toString(s3Object.getObjectContent(), Charset.defaultCharset());
            return data;

        } catch (AmazonServiceException ase) {
            logMessage(ase);
        } catch (AmazonClientException ace) {
            logMessage(ace);
        }
        return data;
    }

    public InputStream getStream(String file)
    {
        String data = "";
        try {
            S3Object s3Object = s3client.getObject(bucketName, file);
            return s3Object.getObjectContent();
        } catch (AmazonServiceException ase) {
            logMessage(ase);
            return null;
        } catch (AmazonClientException ace) {
            logMessage(ace);
            return null;
        }
    }

    public boolean copyFile(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) throws Exception
    {
        try {
            CopyObjectRequest copyObject = new CopyObjectRequest(sourceBucketName,sourceKey,destinationBucketName,destinationKey);
            s3client.copyObject(copyObject);
            return true;
        } catch (AmazonServiceException ase) {
            logMessage(ase);
            return false;
        } catch (AmazonClientException ace) {
            logMessage(ace);
            return false;
        }
    }

    public ObjectListing getFiles(String file)
    {
        ObjectListing objectListing = null;
        try {
            ListObjectsRequest lor = new ListObjectsRequest()
                    .withBucketName(bucketName).withPrefix(file);
            objectListing = s3client.listObjects(lor);
            return objectListing;

        } catch (AmazonServiceException ase) {
            logMessage(ase);
        } catch (AmazonClientException ace) {
            logMessage(ace);
        }
        return objectListing;
    }

    public List<String> getKeys(ObjectListing objectListing)
    {
        List<String> keys = new ArrayList<String>();
        try {

            for (S3ObjectSummary summary: objectListing.getObjectSummaries()) {
                keys.add(summary.getKey());
            }


        } catch (AmazonServiceException ase) {
            logMessage(ase);
        } catch (AmazonClientException ace) {
            logMessage(ace);
        }
        return keys;
    }

    public HashMap<String,String> generatePresignedURLs(List<String> keys)
    {
        HashMap<String,String> signedUrls = new HashMap<String, String>();
        try
        {
            java.util.Date expiration = new java.util.Date();
            long milliSeconds = expiration.getTime();
            milliSeconds += 1000 * urlExpiration; // Add 1 hour.
            expiration.setTime(milliSeconds);

            for(String key:keys) {
                GeneratePresignedUrlRequest generatePresignedUrlRequest =
                        new GeneratePresignedUrlRequest(bucketName, key);
                generatePresignedUrlRequest.setMethod(HttpMethod.GET);
                generatePresignedUrlRequest.setExpiration(expiration);

                URL url = s3client.generatePresignedUrl(generatePresignedUrlRequest);
                signedUrls.put(key, url.toString());
            }
        } catch (AmazonServiceException ase) {
            logMessage(ase);
        } catch (AmazonClientException ace) {
            logMessage(ace);
        }
        return signedUrls;
    }

    public String generatePresignedURL(String key)
    {
        URL url = null;
        try
        {
            java.util.Date expiration = new java.util.Date();
            long milliSeconds = expiration.getTime();
            milliSeconds += 1000 * urlExpiration; // Add 1 hour.
            expiration.setTime(milliSeconds);

            GeneratePresignedUrlRequest generatePresignedUrlRequest =
                    new GeneratePresignedUrlRequest(bucketName, key);
            generatePresignedUrlRequest.setMethod(HttpMethod.GET);
            generatePresignedUrlRequest.setExpiration(expiration);

            url = s3client.generatePresignedUrl(generatePresignedUrlRequest);
        } catch (AmazonServiceException ase) {
            logMessage(ase);
        } catch (AmazonClientException ace) {
            logMessage(ace);
        }
        return url.toString();
    }


    public boolean deleteFolder(String folderName)
    {
        try {
            List<S3ObjectSummary> fileList = s3client.listObjects(bucketName, folderName).getObjectSummaries();
            for (S3ObjectSummary file : fileList) {
                s3client.deleteObject(bucketName, file.getKey());
            }
            s3client.deleteObject(bucketName, folderName);
            return true;
        }catch (AmazonServiceException ase) {
            logMessage(ase);
            return false;
        } catch (AmazonClientException ace) {
            logMessage(ace);
            return false;
        }
    }

    public boolean deleteAllFilesinFolder(String folderName)
    {
        try {
            List<S3ObjectSummary> fileList = s3client.listObjects(bucketName, folderName).getObjectSummaries();
            for (S3ObjectSummary file : fileList) {
                s3client.deleteObject(bucketName, file.getKey());
            }
            return true;
        }catch (AmazonServiceException ase) {
            logMessage(ase);
            return false;
        } catch (AmazonClientException ace) {
            logMessage(ace);
            return false;
        }
    }
}
