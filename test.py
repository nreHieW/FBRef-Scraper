import undetected_chromedriver as uc
from pyvirtualdisplay import Display
from selenium_stealth import stealth

display = Display(visible=0, size=(800, 800))
display.start()

driver = uc.Chrome(headless=False, use_subprocess=False)
stealth(
    driver,
    languages=["en-US", "en"],
    vendor="Google Inc.",
    platform="Win32",
    webgl_vendor="Intel Inc.",
    renderer="Intel Iris OpenGL Engine",
    fix_hairline=True,
)
driver.get("https://whoscored.com/")
print(driver.page_source)
