package qa.automation.aws;

import io.restassured.response.Response;
import java.io.File;
import static io.restassured.RestAssured.given;

/**
 * Created by johnson_phillips on 1/27/18.
 */
public class S3UploadHandler extends Thread {

    public static String s3PathandName;
    public static File file;
    public static String url = "";
    public static boolean printconsole = false;

    @Override
    public void run()
    {
        try {
            Response response = given().request().multiPart(file).post(url + s3PathandName).andReturn();
            if(printconsole)
            {
                response.prettyPrint();
            }

        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        super.stop();
    }

}
