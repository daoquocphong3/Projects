# Ho Chi Minh Restaurant Analysis
## Crawl Foody web:
### Crawl Restaurant's Infomation: [1.1 hcmc_foody.ipynb](https://github.com/daoquocphong3/Projects/blob/main/Restaurant%20Analysis%20(long%20project)/Code/1.1%20hcmc_foody.ipynb)
- Using Requests calling API of search page of Foody to get list of Restaurant. Because of the limited result, we split API by parameter district and category of the API. Extract restaurant ID from the list.
- Using BeautifulSoup to get full detail of the restaurant from the Restaurant detail page.
- <ins>**Result**</ins>: A table contains 101.401 rows and 33 columns(close to 130K restaurant in HCM at that time).

![image](https://user-images.githubusercontent.com/55779400/220302237-8b923a38-cbe9-48ef-9eaa-23b12661e80a.png)

### Crawl Comment Of Restaurant: [1.2 - Foody - Crawl cmt.ipynb](https://github.com/daoquocphong3/Projects/blob/main/Restaurant%20Analysis%20(long%20project)/Code/1.2%20-%20Foody%20-%20Crawl%20cmt.ipynb)
- Using Request call API, and json_normalize to transform json result into Pandas table.(only crawl for 60K restaurant intersection of Foody and ShopeeFood)
- Using Threading to speed-up the process.
- <ins>**Result**</ins>: A table contains 319.378 rows and 64 columns.
   
![image](https://user-images.githubusercontent.com/55779400/220302474-eddd8bfc-079b-4321-953f-05fe92676b1e.png)
## Crawl ShopeeFood App:
### Get API of ShoopeeFood App: [Reference](https://www.xbyte.io/how-to-scrape-data-from-mobile-apps.php)
- Using Android Virtual Device like [Android Developer Studio](https://developer.android.com/studio/) and HTTPS proxy like [Mitmproxy](https://mitmproxy.org/) 


![image](https://user-images.githubusercontent.com/55779400/220056980-810b1316-4fa5-4456-a971-312821c33a56.png)
