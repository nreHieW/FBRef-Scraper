import undetected_chromedriver as uc
from pyvirtualdisplay import Display

display = Display(visible=0, size=(800, 800))
display.start()

driver = uc.Chrome(headless=False, use_subprocess=False)
driver.get("https://whoscored.com/")
print(driver.page_source)
