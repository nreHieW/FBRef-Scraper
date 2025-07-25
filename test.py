import undetected_chromedriver as uc
from pyvirtualdisplay import Display
from selenium.webdriver import ChromeOptions

display = Display(visible=0, size=(800, 800))
display.start()
options = ChromeOptions()
options.add_argument("--proxy-server={}".format("8.219.97.248:80"))
driver = uc.Chrome(headless=False, use_subprocess=False)
driver.get("https://whoscored.com/")
print(driver.page_source)
