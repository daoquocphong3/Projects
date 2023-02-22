<!-- # Ho Chi Minh Restaurant Analysis -->
## Crawl Foody web:
### Crawl Restaurant's Infomation: [1.1 hcmc_foody.ipynb](https://github.com/daoquocphong3/Projects/blob/main/Restaurant%20Analysis%20(long%20project)/Code/1.1%20hcmc_foody.ipynb)
- Using Requests calling API of search page of Foody to get list of Restaurant. Because of the limited result, we split API by parameter district and category of the API. Extract restaurant ID from the list.
- Using BeautifulSoup to get full detail of the restaurant from the Restaurant detail page.
- <ins>**Result**</ins>: A table contains 101.401 rows and 33 columns(close to 130K restaurant in HCM at that time).

![image](https://user-images.githubusercontent.com/55779400/220302237-8b923a38-cbe9-48ef-9eaa-23b12661e80a.png)

### Crawl Comment Of Restaurant: [1.2 - Foody - Crawl cmt.ipynb](https://github.com/daoquocphong3/Projects/blob/main/Restaurant%20Analysis%20(long%20project)/Code/1.2%20-%20Foody%20-%20Crawl%20cmt.ipynb)
- Using Requests.get() call API, and json_normalize to transform json result into Pandas table.(only crawl for 60K restaurant intersection of Foody and ShopeeFood)
- Using MultiThreading to speed-up the process.
- <ins>**Result**</ins>: A table contains 319.378 rows and 64 columns.
   
![image](https://user-images.githubusercontent.com/55779400/220302474-eddd8bfc-079b-4321-953f-05fe92676b1e.png)
## Crawl ShopeeFood App:
### Get API of ShoopeeFood App: [Reference](https://www.xbyte.io/how-to-scrape-data-from-mobile-apps.php)
- Get APIs using Android Virtual Device like [Android Developer Studio](https://developer.android.com/studio/) and HTTPS proxy like [Mitmproxy](https://mitmproxy.org/) 
- Use [Appium](https://appium.io/) to control the Virtual Android.
- <ins>**Result**</ins>: Some API of ShopeeFood.

### Extract Restaurant IDs: [2.1 Scan location.ipynb](https://github.com/daoquocphong3/Projects/blob/main/Restaurant%20Analysis%20(long%20project)/Code/2.1%20Scan%20location.ipynb)
- Using [Nominatim](https://nominatim.org/) to get a grid of longtitudes and lattitudes of HCM city like: 

![image](https://user-images.githubusercontent.com/55779400/220310132-475f8532-1925-42fe-8a2e-b707eee2eea3.png)

- Using Requests.post() call API of ShopeeFood to get IDs of restaurants.
- <ins>**Result**</ins>: 67.229 Restaurant IDs and it's longtitude, lattitude.

![image](https://user-images.githubusercontent.com/55779400/220312115-505a12f0-e95f-4ac5-8eef-19657bd9e217.png)

### Crawl Restaurant's Information: [2.2 Shopee - Crawl restaurants.ipynb](https://github.com/daoquocphong3/Projects/blob/main/Restaurant%20Analysis%20(long%20project)/Code/2.2%20Shopee%20-%20Crawl%20restaurants.ipynb)
- Using Request.get() call API of ShopeeFood.
- <ins>**Result**</ins>: A table contains 80.072 rows and 21 columns. 

![image](https://user-images.githubusercontent.com/55779400/220314126-40f3da9d-c976-402f-aeee-1c6b3b1a5b38.png)

### Crawl Dishes Information: [2.3 Shopee - Crawl dishes.ipynb](https://github.com/daoquocphong3/Projects/blob/main/Restaurant%20Analysis%20(long%20project)/Code/2.3%20Shopee%20-%20Crawl%20dishes.ipynb)
- Using Request.get() call API of ShopeeFood.
- <ins>**Result**</ins>: A table contains 3.090.871 rows and 19 columns.

![image](https://user-images.githubusercontent.com/55779400/220380522-7de71b22-67ed-410d-a76b-4e0663a1e5b5.png)



### Crawl Topping Data for Dishes: [2.4 - Shopee - CrawlToppingData.ipynb](https://github.com/daoquocphong3/Projects/blob/main/Restaurant%20Analysis%20(long%20project)/Code/2.4%20-%20Shopee%20-%20CrawlToppingData.ipynb)
- Using MultiThreading and Request.get() call API of ShopeeFood to get topping of 1.867.802 dishes(from the intersection Restaurant).
- Flatten and transform the json result into pandas dataframe.
- <ins>**Result**</ins>: A table contains 3.295.914  rows and 22 columns.

![image](https://user-images.githubusercontent.com/55779400/220430529-00002ad7-d7e5-48c5-b167-12d84556a85d.png)

### Optimize Crawling process: [2.5 Shopee - Optimzie crawl restaurants and dishes.ipynb](https://github.com/daoquocphong3/Projects/blob/main/Restaurant%20Analysis%20(long%20project)/Code/2.5%20Shopee%20-%20Optimzie%20crawl%20restaurants%20and%20dishes.ipynb)
- Using MultiThreading to speed-up crawling process for Shopee.
- Load all data into parquet format to reduce size of data.
- <ins>**Result**</ins>: All crawling process used to consume **20 hours** can run within **2 hours**. 

### EDA And Model Data For Better Analyzing And Visualization: 
### [2.6 Shopee - EDA and Unify parquets.ipynb](https://github.com/daoquocphong3/Projects/blob/main/Restaurant%20Analysis%20(long%20project)/Code/2.6%20Shopee%20-%20EDA%20and%20Unify%20parquets.ipynb), [2.7 Shopee - EDA and Extract parquets.ipynb](https://github.com/daoquocphong3/Projects/blob/main/Restaurant%20Analysis%20(long%20project)/Code/2.7%20Shopee%20-%20EDA%20and%20Extract%20parquets.ipynb)
- Cleaning data: remove unwanted data, examinate the outlier, only choice valuable attribute to put into the SQLite3 database.
- Gain overview and basic knowledge about the data.

## Data Modeling: [3.1 Data Modeling.ipynb](https://github.com/daoquocphong3/Projects/blob/main/Restaurant%20Analysis%20(long%20project)/Code/3.1%20Data%20Modeling.ipynb)
- Merge data from Foody and ShopeeFood app.
- Design and create table schema for data.
- Overall structure of the [Database](https://drive.google.com/file/d/1YipU3eMJLLDLgcV8MGhxXXpRcvfC95lg/view):

![diagram](https://user-images.githubusercontent.com/55779400/220547979-c2ed3419-49de-4510-b587-30571ea2813a.png)

## Visualization:
- We create [Power BI](https://app.powerbi.com/groups/me/reports/1925003b-32bb-463b-b593-7d7307fd9810?ctid=40127cd4-45f3-49a3-b05d-315a43a9f033&pbi_source=linkShare&bookmarkGuid=f9b5fb9e-dd14-4c96-a3f6-ca390ce32da8) ( [Power BI file](https://drive.google.com/file/d/1qWlmexHhkaAQj391D9NMRutz9Boj3MvC/view?usp=share_link) if my account expires) dashboard to help user find the dish they like by dish name or the address they are in.

![image](https://user-images.githubusercontent.com/55779400/220550393-48be7dc2-479f-40b0-85cf-0f487960cad6.png)


![image](https://user-images.githubusercontent.com/55779400/220056980-810b1316-4fa5-4456-a971-312821c33a56.png)

## Conlusion:
- There are still a lots of attribute we have not inspected yet and a lot more insight can get from the dataset.
- The data model we provide can be use for more analysing later on.
