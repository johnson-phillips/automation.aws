import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import automation.aws.S3;

public class S3Test {
    S3 s3;

    @BeforeSuite
    public void beforeSuite() {
        s3 = new S3(System.getProperty("aws_key"),System.getProperty("aws_secret"),System.getProperty("aws_bucket"));
    }

    @Test
    public void unitTest() throws Exception {
        s3.createFolder("unit_test");
        s3.uploadFile("unit_test/test.json","{\"topic\":\"topic name\",\"value\":\"value\"}");
        System.out.println(s3.getFile("unit_test/test.json"));
        s3.deleteFile("unit_test/test.json");
    }
}
