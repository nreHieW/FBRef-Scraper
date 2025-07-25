import undetected_chromedriver as uc

driver = uc.Chrome(headless=False, use_subprocess=False)
driver.get("https://whoscored.com/")
print(driver.page_source)
