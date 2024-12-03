from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO, 
    handlers=[
        logging.StreamHandler()  # Ensures logs are sent to stdout/stderr
    ]
)
logger = logging.getLogger('knowledge_base.py')


map_eng = {"C2HD":"You can make a booking with us to lock-in the special price, then later complete your payment when you've received your service at the clinic or hospital by contacting us again",
       "C2HD (Flow/Mena)":"You can make a booking with us to lock-in the special price, then later complete your payment when you've received your service at the clinic or hospital by contacting us again",
       "C2HD AND PAY AT CLINIC":"You can make a booking with us to lock-in the special price, then later complete your payment when you've received your service at the clinic or hospital by contacting us again",
       "PAY AT CLINIC ONLY":"You can make a booking with us to lock-in the special price, then later complete your payment directly at the clinic or hospital",
       "PAY AT HD AND CLINIC":"You can pay online through the product link or request an agent to help process your order",
       "PAY AT HD ONLY":"You can pay online through the product link or request an agent to help process your order",
       "PAY AT YANHEE ONLY (get cashback)":"You can make a booking with us to lock-in the special price, then later complete your payment directly at the clinic or hospital",
       "PAY AT YANHEE ONLY (no cashback)":"You can make a booking with us to lock-in the special price, then later complete your payment directly at the clinic or hospital",
       "SOCIAL SECURITY":"You can make a booking with us to lock-in the special price, then later complete your payment at the clinic or hospital by filing your social security claim there"}

map_thai = {"C2HD":"จองก่อนจ่ายทีหลัง - นำคูปองไปยื่นในวันรับบริการ แล้วค่อยกลับมาจ่ายกับเราในราคาพิเศษ คูปองมีอายุ 30 วันนับจากวันที่ได้รับ อย่าลืมไปใช้บริการนะคะ",
       "C2HD (Flow/Mena)":"จองก่อนจ่ายทีหลัง - นำคูปองไปยื่นในวันรับบริการ แล้วค่อยกลับมาจ่ายกับเราในราคาพิเศษ คูปองมีอายุ 30 วันนับจากวันที่ได้รับ อย่าลืมไปใช้บริการนะคะ",
       "C2HD AND PAY AT CLINIC":"จองก่อนจ่ายทีหลัง - นำคูปองไปยื่นในวันรับบริการ แล้วค่อยกลับมาจ่ายกับเราในราคาพิเศษ คูปองมีอายุ 30 วันนับจากวันที่ได้รับ อย่าลืมไปใช้บริการนะคะ",
       "PAY AT CLINIC ONLY":"จองกับเรา แต่จ่ายที่คลินิก/รพ. - เพียงนำคูปองไปยื่นในวันรับบริการและจ่ายเงินในราคาพิเศษ คูปองมีอายุ 30 วันนับจากวันที่ได้รับ อย่าลืมไปใช้บริการนะคะ",
       "PAY AT HD AND CLINIC":"จองและจ่ายผ่าน HDmall - จะกดจ่ายเงินออนไลน์ที่หน้าสินค้า หรือจ่ายผ่านแอดมินก็ได้ คูปองมีอายุ 60 วันนับจากวันที่ซื้อ อย่าลืมไปใช้บริการนะคะ",
       "PAY AT HD ONLY":"จองและจ่ายผ่าน HDmall - จะกดจ่ายเงินออนไลน์ที่หน้าสินค้า หรือจ่ายผ่านแอดมินก็ได้ คูปองมีอายุ 60 วันนับจากวันที่ซื้อ อย่าลืมไปใช้บริการนะคะ",
       "PAY AT YANHEE ONLY (get cashback)":"จองกับเรา แต่จ่ายที่รพ. - เพียงนำคูปองไปยื่นในวันที่รับบริการ จ่ายเงิน และนำใบเสร็จมารับ Cashback กับ HDmall คูปองมีอายุ 30 วันนับจากวันที่ได้รับ อย่าลืมไปใช้บริการนะคะ",
       "PAY AT YANHEE ONLY (no cashback)":"จองกับเรา แต่จ่ายที่รพ. - สามารถยื่นคูปองและจ่ายเงินในวันที่รับบริการได้เลย คูปองมีอายุ 30 วันนับจากวันที่ได้รับ อย่าลืมไปใช้บริการนะคะ",
       "SOCIAL SECURITY":"จองกับเรา และทำเรื่องเบิกประกันสังคมที่คลินิก/รพ. ในวันที่รับบริการ คูปองมีอายุ 30 วันนับจากวันที่ได้รับ อย่าลืมไปใช้บริการนะคะ"}

def concat_sheets(df_main, df_cash, df_feed, df_payment):
    # Find unmatched URLs using set operations
    cash_urls = set(df_cash['SKU URL'])
    main_urls = set(df_main['url'])

    # URLs in df_cash that aren't in df_main
    unmatched_cash = cash_urls - main_urls

    # URLs in df_main that aren't in df_cash
    unmatched_main = main_urls - cash_urls

    # Still do the assignments
    for i in range(len(df_cash)):
        url = df_cash.loc[i, 'SKU URL']
        df_main.loc[df_main['url'] == url, 'cash_discount'] = df_cash.loc[i,'Cash Discount']

    for i in range(len(df_main)):
        df_main.loc[i, 'price_after_cash_discount'] = df_main.loc[i,'price'] - df_main.loc[i,'cash_discount']

    df_main['original_price'] = df_main['price'].astype(str)

    for i in range(len(df_main)):
        query = df_main.loc[i,'package_name']
        try:
            price = df_feed.loc[df_feed['title'] == query]['price'].values[0]
            df_main.loc[i, 'original_price'] = str(price)
        except IndexError:
            price = None


    df_main['payment_method_eng'] = ''
    df_main['payment_method_thai'] = ''

    count = 0
    for i in range(len(df_main)):
        url = df_main.loc[i, 'url']
        try:
            # First check if URL exists in df_payment
            matching_rows = df_payment[df_payment['Package URL']==url]
            if matching_rows.empty:
                #print(f"Row {i}: No matching URL found: {url}")
                count+=1
                continue

            # Try to get the payment method
            try:
                key = matching_rows['Payment Method'].values[0]
            except IndexError:
                #print(f"Row {i}: Payment Method column is empty for URL: {url}")
                continue

            # Try to map the payment method
            if key not in map_thai:
                #print(f"Row {i}: Payment key '{key}' not found in map_thai dictionary")
                continue
            if key not in map_eng:
                #print(f"Row {i}: Payment key '{key}' not found in map_eng dictionary")
                continue

            # If all checks pass, update the dataframe
            df_main.loc[i, 'payment_method_thai'] = map_thai[key]
            df_main.loc[i,'payment_method_eng'] = map_eng[key]

        except Exception as e:
            print(f"Row {i}: Unexpected error: {str(e)}")
            # Optionally print more debug info
            print(f"URL: {url}")
            print(f"Matching rows in df_payment: {len(matching_rows) if 'matching_rows' in locals() else 'not checked yet'}")
            continue

    return df_main
