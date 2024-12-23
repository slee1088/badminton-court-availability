#from playwright.sync_api import sync_playwright

#with sync_playwright() as p:
#	for browser_type in [p.chromium]:
#		browser = browser_type.launch()
#		page = browser.new_page()
#		page.goto('https://alphabadminton.yepbooking.com.au/')
#		close_button = page.query_selector(".ui-button-text-only")
#		close_button.click()
#		page.screenshot(path=f'example1-{browser_type.name}.png')
#		browser.close()
        
        
        
from playwright.sync_api import sync_playwright

playwright = sync_playwright().start()
# Use playwright.chromium, playwright.firefox or playwright.webkit
# Pass headless=False to launch() to see the browser UI
browser = playwright.chromium.launch()
page = browser.new_page()
page.goto('https://alphabadminton.yepbooking.com.au/')

try: 
  close_button = page.query_selector(".ui-button-text-only")
  close_button.click()
except:
  print("button already clicked")

page.get_by_text("Alpha Auburn (22 Courts)").click()
try: 
  close_button = page.query_selector(".ui-button-text-only")
  close_button.click()
except:
  print("button already clicked")

with open('auburn.html', "w+", encoding="utf-8") as f:
	f.write(page.content())
browser.close()
playwright.stop()
