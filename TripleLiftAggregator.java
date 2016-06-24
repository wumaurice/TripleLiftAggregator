import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Aggreagtes data from Advertisers loaded from json by date
 *
 * Created by Maurice Wu on 6/23/16.
 */
public class TripleLiftAggregator implements OnDataRetrievedListener
{
    // ================================================================================
    // Properties
    // ================================================================================

    private static final String ADVERTISER_BASE_URL = "http://dan.triplelift.net/code_test.php?advertiser_id=";
    private static final int TIMEOUT_MILLISECONDS = 200;

    OnDataRetrievedListener onDataRetrievedListener;
    int receivedDataCount = 0;

    // ================================================================================
    // Custom Models
    // ================================================================================

    /**
     * Class that contains the final aggregated data
     */
    public class RetrievedData
    {
        List<AdvertiserData> aggregatedData;

        RetrievedData()
        {
            aggregatedData = new ArrayList<>();
        }
    }

    /**
     * Model that contains the data for an advertiser: the date, num clicks, and num impressions
     */
    private class AdvertiserData
    {
        String date;
        int numClicks;
        int numImpressions;

        /**
         * @param json contains the data of the model under json
         */
        AdvertiserData(JSONObject json)
        {
            try
            {
                date = json.getString("ymd");
                numClicks = json.getInt("num_clicks");
                numImpressions = json.getInt("num_impressions");
            }
            catch (JSONException e)
            {
                e.printStackTrace();
            }
        }

        public String getDate()
        {
            return date;
        }
    }

    /**
     * Model of an advertiser: its id and the data of an advertiser
     */
    private class Advertiser
    {
        int id;
        AdvertiserData advertiserData;

        Advertiser(JSONObject json)
        {
            try
            {
                id = json.getInt("advertiser_id");
                advertiserData = new AdvertiserData(json);
            }
            catch (JSONException e)
            {
                e.printStackTrace();
            }
        }

        public AdvertiserData getAdvertiserData()
        {
            return advertiserData;
        }
    }

    /**
     * Thread that basically make load the data from an url
     */
    public class FetchJsonThread implements Runnable
    {
        private final String url;
        private final OnJsonLoadedListener onJsonLoadedListener;

        /**
         * Constructor
         *
         * @param url                  the URL to load the json data from
         * @param onJsonLoadedListener the event listner when the data has been retrieved or when the connection failed
         */
        FetchJsonThread(String url, OnJsonLoadedListener onJsonLoadedListener)
        {
            this.url = url;
            this.onJsonLoadedListener = onJsonLoadedListener;
        }

        @Override
        public void run()
        {
            try
            {
                URL siteURL = new URL(url);

                // Make connection
                URLConnection connection = null;
                connection = siteURL.openConnection();
                connection.setConnectTimeout(TIMEOUT_MILLISECONDS);
                connection.setDoOutput(true);
                connection.setAllowUserInteraction(false);

                // Get result
                BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String line;
                String dataString = "";
                while ((line = br.readLine()) != null)
                {
                    dataString += line;
                }
                br.close();

                if (onJsonLoadedListener != null)
                {
                    // Data has been retrieved in a json form
                    onJsonLoadedListener.onReceiveJsonDataString(dataString);
                }
            }
            catch (Exception e)
            {
                if (onJsonLoadedListener != null)
                {
                    // Something went wrong and no data has been retrieved
                    onJsonLoadedListener.onReceiveJsonDataString(null);
                }
            }
        }
    }

    // ================================================================================
    // Constructor
    // ================================================================================

    /**
     * Constructor
     */
    public TripleLiftAggregator()
    {
        onDataRetrievedListener = this;
    }

    // ================================================================================
    // Helper Methods
    // ================================================================================

    /**
     * Loads all the urls, retrieves the json asynchronously and then aggregates the data
     *
     * @param advertiserIds List of all the advertiser ids that we want to retrieve
     */
    private void loadDataForAdvertiser(final long[] advertiserIds)
    {
        loadDataForAdvertiser(advertiserIds, 8);
    }

    /**
     * Loads all the urls, retrieves the json asynchronously and then aggregates the data
     *
     * @param advertiserIds List of all the advertiser ids that we want to retrieve
     * @param numThreadsMax Maximun number of threads to run concurrently
     */
    private void loadDataForAdvertiser(final long[] advertiserIds, int numThreadsMax)
    {
        if (advertiserIds != null && advertiserIds.length > 0)
        {
            final List<Advertiser> advertisers = new ArrayList<>();

            // Creates an executor than has a fixed size thread pool of 5 workers threads
            // Holds a queue of the size of the number of urls
            ExecutorService executor = new ThreadPoolExecutor(2, numThreadsMax, 5L, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<Runnable>(Math.min(advertiserIds.length, Integer.MAX_VALUE)));

            for (int i = 0; i < advertiserIds.length; i++)
            {
                // Run the thread
                Runnable worker = new FetchJsonThread(ADVERTISER_BASE_URL + advertiserIds[i], new OnJsonLoadedListener()
                {
                    @Override
                    public void onReceiveJsonDataString(String dataString)
                    {
                        if (dataString != null)
                        {
                            try
                            {
                                // Converts the data string to a JSONArray
                                JSONArray jsonArray = new JSONArray(dataString);
                                for (int i = 0; i < jsonArray.length(); i++)
                                {
                                    // Iterates trough each json object and converts it to our Advertiser model
                                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                                    advertisers.add(new Advertiser(jsonObject));
                                }
                            }
                            catch (JSONException e)
                            {
                                e.printStackTrace();
                            }
                        }
                        receivedDataCount++;

                        // We received a response from all the url, we can start sorting/aggregating data
                        if (receivedDataCount == advertiserIds.length)
                        {
                            aggregateData(advertisers);
                        }
                    }
                });
                executor.execute(worker);
            }
            executor.shutdown();
        }
    }

    /**
     * Aggregates the advertiser data
     *
     * @param advertisers List of the advertisers retrieved from the urls
     */
    private void aggregateData(List<Advertiser> advertisers)
    {
        List<AdvertiserData> aggregatedData = new ArrayList<>();
        for (int i = 0; i < advertisers.size(); i++)
        {
            AdvertiserData advertiserData = advertisers.get(i).getAdvertiserData();

            // Check if the date exists if the aggregarted data list
            AdvertiserData existentData = getAdvertiserDataByDate(aggregatedData, advertiserData.getDate());
            if (existentData == null)
            {
                // The date of the advertiser data ins't present in the aggregated list so we jsut add the data in the list
                aggregatedData.add(advertiserData);
            }
            else
            {
                int indexOfExistentData = aggregatedData.indexOf(existentData);
                // The date is already present, so we sum the numClicks and numImpressions with the advertiser
                aggregatedData.get(indexOfExistentData).numClicks += advertiserData.numClicks;
                aggregatedData.get(indexOfExistentData).numImpressions += advertiserData.numImpressions;
            }
        }

        // Create RetrievedData model and pass it in the onDataRetrieved method
        RetrievedData retrievedData = new RetrievedData();
        retrievedData.aggregatedData.addAll(aggregatedData);
        onDataRetrievedListener.onDataRetrieved(retrievedData);
    }

    /**
     * Checks if the date is present in the list
     *
     * @param advertiserDataList List of the advertiser data to check the date in
     * @param date               The date to check
     * @return The advertiser data that has the date provided if found, null otherwise
     */
    private AdvertiserData getAdvertiserDataByDate(List<AdvertiserData> advertiserDataList, String date)
    {
        for (AdvertiserData advertiserData : advertiserDataList)
        {
            if (advertiserData.getDate().equals(date))
            {
                return advertiserData;
            }
        }
        return null;
    }

    // ================================================================================
    // OnDataRetrievedListener Implentation
    // ================================================================================

    @Override
    public void onDataRetrieved(RetrievedData data)
    {
        for (AdvertiserData advertiserData : data.aggregatedData)
        {
            // Prints the aggregated data by date
            System.out.println(String.format(Locale.US, "%s:\tClicks: %d\tImpressions:%d", advertiserData.getDate(), advertiserData.numClicks, advertiserData.numImpressions));
        }
    }

    // ================================================================================
    // Main
    // ================================================================================

    public static void main(String[] args)
    {
        // Test
        TripleLiftAggregator aggregator = new TripleLiftAggregator();
        aggregator.loadDataForAdvertiser(new long[] {123, 124, 456, 457, 726});
    }
}

/**
 * Listener called when the json has been retrieved
 */
interface OnJsonLoadedListener
{
    void onReceiveJsonDataString(String jsonDataString);
}

/**
 * Listener called when all the data has been aggregated
 */
interface OnDataRetrievedListener
{
    void onDataRetrieved(TripleLiftAggregator.RetrievedData data);
}
