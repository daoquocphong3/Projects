<!-- # Ho Chi Minh Restaurant Analysis -->
## Crawl Foody web:
### Crawl Restaurant's Infomation: [1.1 hcmc_foody.ipynb](https://github.com/daoquocphong3/Projects/blob/main/Restaurant%20Analysis%20(long%20project)/Code/1.1%20hcmc_foody.ipynb)
- Using Requests calling API of search page of Foody to get list of Restaurant. Because of the limited result, we split API by parameter district and category of the API. Extract restaurant ID from the list.
- Using BeautifulSoup to get full detail of the restaurant from the Restaurant detail page.
- <ins>**Result**</ins>: A table contains 101.401 rows and 33 columns(close to 130K restaurant in HCM at that time).

![image](https://user-images.githubusercontent.com/55779400/220302237-8b923a38-cbe9-48ef-9eaa-23b12661e80a.png)

### Crawl Comment Of Restaurant: [1.2 - Foody - Crawl cmt.ipynb](https://github.com/daoquocphong3/Projects/blob/main/Restaurant%20Analysis%20(long%20project)/Code/1.2%20-%20Foody%20-%20Crawl%20cmt.ipynb)
- Using Requests.get() call API, and json_normalize to transform json result into Pandas table.(only crawl for 60K restaurant intersection of Foody and ShopeeFood)
- Using Threading to speed-up the process.
- <ins>**Result**</ins>: A table contains 319.378 rows and 64 columns.
   
![image](https://user-images.githubusercontent.com/55779400/220302474-eddd8bfc-079b-4321-953f-05fe92676b1e.png)
## Crawl ShopeeFood App:
### Get API of ShoopeeFood App: [Reference](https://www.xbyte.io/how-to-scrape-data-from-mobile-apps.php)
- Get APIs using Android Virtual Device like [Android Developer Studio](https://developer.android.com/studio/) and HTTPS proxy like [Mitmproxy](https://mitmproxy.org/) 
- Use [Appium](https://appium.io/) to control the Virtual Android.
- <ins>**Result**</ins>: Some API of ShopeeFood.

### Extract Restaurant IDs:
- Using [Nominatim](https://nominatim.org/) to get a grid of longtitudes and lattitudes of HCM city like: 

![image](https://user-images.githubusercontent.com/55779400/220310132-475f8532-1925-42fe-8a2e-b707eee2eea3.png)

- Using Requests.post() call API of ShopeeFood to get IDs of restaurants.
- <ins>**Result**</ins>: 67.229 Restaurant IDs and it's longtitude, lattitude.

![image](https://user-images.githubusercontent.com/55779400/220312115-505a12f0-e95f-4ac5-8eef-19657bd9e217.png)

### Crawl Restaurant's Information:
- Using Request.get() call API of ShopeeFood.
- <ins>**Result**</ins>: A table contains 80.072 rows and 22 columns. 

![image](https://user-images.githubusercontent.com/55779400/220314126-40f3da9d-c976-402f-aeee-1c6b3b1a5b38.png)

### Crawl Dishes Information:
- Using Request.get() call API of ShopeeFood.
- <ins>**Result**</ins>: A table contains 3.090.871 rows and 20 columns.

![image](https://user-images.githubusercontent.com/55779400/220380522-7de71b22-67ed-410d-a76b-4e0663a1e5b5.png)



### Crawl Topping Data for Dishes: 
- Get



![image](https://user-images.githubusercontent.com/55779400/220056980-810b1316-4fa5-4456-a971-312821c33a56.png)
