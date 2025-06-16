# src/utils/waitdrivermanager.py

from typing import Callable, Tuple, Union

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class WaitHelper:
    """Class helper for managing smart waits"""

    @staticmethod
    def wait_for(
        driver,
        condition: Union[Tuple[str, str], Callable],
        timeout: float = 10,
        poll_frequency: float = 0.5,
        ignored_exceptions: tuple = None,
        message: str = "",
    ) -> bool:
        """
        Timeout for of condition possibly with message
        Args:
        driver: WebDriver instance
        condition: Can be:
            - Tuple (By, locator) to wait for element
            - Function that returns bool
            - EC (expected_condition)
        timeout: Maximum wait time (seconds)
        poll_frequency: Interval between attempts (seconds)
        ignored_exceptions: Exceptions to ignore
        message: Custom message for timeout
        Returns:
            bool: True if condition was met
        Raises:
            TimeoutException: If condition is not met
        """
        wait = WebDriverWait(
            driver,
            timeout=timeout,
            poll_frequency=poll_frequency,
            ignored_exceptions=ignored_exceptions,
        )

        return wait.until(condition, message=message)

    @staticmethod
    def wait_for_element(
        driver,
        by: str,
        locator: str,
        timeout: float = 10,
        visible: bool = False,
        clickable: bool = False,
    ):
        """
        Wait for a specific element
        Args:
            driver: WebDriver instance
            by: Location strategy (By.ID, By.XPATH, etc.)
            locator: Element locator
            timeout: Maximum wait time
            visible: If True, wait for visible element
            clickable: If True, wait for clickable element
        """
        if clickable:
            condition = EC.element_to_be_clickable((by, locator))
        elif visible:
            condition = EC.visibility_of_element_located((by, locator))
        else:
            condition = EC.presence_of_element_located((by, locator))

        return WaitHelper.wait_for(driver, condition, timeout=timeout)

    @staticmethod
    def wait_for_elements(
        driver,
        by: str,
        locator: str,
        timeout: float = 10,
        visible: bool = False,
    ):
        condition = EC.presence_of_all_elements_located((by, locator))

        if visible:
            condition = EC.visibility_of_any_elements_located((by, locator))

        return WaitHelper.wait_for(driver, condition, timeout=timeout)

    @staticmethod
    def wait_for_page_load(driver, timeout: float = 30):
        def page_loaded(drv):
            return (
                drv.execute_script("return document.readyState") == "complete"
            )

        WaitHelper.wait_for(
            driver,
            page_loaded,
            timeout=timeout,
            message="Timeout ao carregar a página",
        )

    @staticmethod
    def wait_for_element_disappear(
        driver, by: str, locator: str, timeout: float = 10
    ):
        """Espera até que um elemento desapareça"""
        WaitHelper.wait_for(
            driver,
            EC.invisibility_of_element_located((by, locator)),
            timeout=timeout,
        )
