import sys
import time
import json
from pathlib import Path

import click
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support.core import std_path

TEMPLATE_LOGIN = {
    'fields': {
        'username': {
            'kind': 'text',
            'selector': [
                'input[name="login"]',
                'input[name="loginForm:loginName"]'
            ]
        },
        'password': {
            'kind': 'text',
            'selector': [
                'input[type="password"]',
                'input[name="loginForm:password"]'
            ]
        }
    },
    'buttons': {
        'submit':['input[type="submit"][value="Login"]', 'button[name="loginForm:fbtnLogin"]']
    }
}

TEMPLATE_DOCKET_SHEET = {
    'fields': {
        'case_no': {
            'kind': 'text',
            'selector': '[name="case_num"]',
            'default': '',
            'type': str
        },
        'filed_entered':{
            'kind': 'radio',
            'selector': 'input[type="radio"][name="date_range_type"]',
            'choices': ['Filed', 'Entered'],
            'default': 'Filed',
            'type': str
        },
        'date_from':{
            'kind': 'text',
            'selector': 'input[name="date_from"]',
            'default': '',
            'help': 'MM/DD/YYYY',
            'type': str
        },
        'date_to':{
            'kind': 'text',
            'selector': 'input[name="date_to"]',
            'default': '',
            'help': 'MM/DD/YYYY',
            'type': str
        },
        'docs_from': {
            'kind': 'text',
            'selector': 'input[name="documents_numbered_from_"]',
            'default': '',
            'type': str
        },
        'docs_to': {
            'kind': 'text',
            'selector': 'input[name="documents_numbered_to_"]',
            'default': '',
            'type': str
        },
        'go_to_doc': {
            'kind': 'text',
            'selector': 'input[name="document_number"]',
            'default': '',
            'type': str
        },
        'page_id': {
            'kind': 'text',
            'selector': 'input[name="display_pageid"]',
            'default': '',
            'type': str
        },
        'include_parties': {
            'kind': 'checkbox',
            'selector': 'input#list_of_parties_and_counsel',
            'default': True,
            'type': bool
        },
        'include_terminated': {
            'kind': 'checkbox',
            'selector': 'input#terminated_parties',
            'default': True,
            'type': bool
        },
        'include_list_member_cases': {
            'kind': 'checkbox',
            'selector': 'input#list_of_member_cases',
            'default': False,
            'type': bool
        },
        'include_headers': {
            'kind': 'checkbox',
            'selector': 'input#pdf_header',
            'default': True,
            'type': bool
        },
        'view_multiple': {
            'kind': 'checkbox',
            'selector': 'input#view_multi_docs',
            'default': False,
            'type': bool,
            'help': 'Cannot also specify format if this is True'
        },
        'format': {
            'kind': 'radio',
            'selector': 'input[name="output_format"]',
            'choices': ['html', 'pdf'],
            'default': 'html'
        },
        'sort_by': {
            'kind': 'select',
            'selector': 'select[name="sort1"]',
            'choices': ['oldest date first', 'most recent date first', 'document number'],
            'default': 'html'
        },
    },
    'buttons': {
        'submit': 'input[value^="Run"]'
    },
    'pre_submit': lambda form: case_no_pre_submit(form)
}

TEMPLATE_QUERY = {
    'fields': {
        'case_no': {
            'kind': 'text',
            'selector': 'input[name="case_num"]',
            'type': str,
            'example': '8:16-cv-00750',
            'default': ''
        },
        'case_status': {
            'kind': 'radio',
            'selector': 'input[type="radio"][name="case_status"]',
            'type': str,
            'example': 'open',
            'choices':["open", "closed", "all"],
            'default': 'all'
        },
        'filed_from':{
            'kind': 'text',
            'selector': 'input[type="text"][name="Qry_filed_from"]',
            'type': str,
            'example': '07/20/2020',
            'help': 'MM/DD/YYYY',
            'default': ''
        },
        'filed_to':{
            'kind': 'text',
            'selector': 'input[type="text"][name="Qry_filed_to"]',
            'type': str,
            'example': '07/21/2020',
            'help': 'MM/DD/YYYY',
            'default': ''
        },
        'last_entry_from':{
            'kind': 'text',
            'selector': 'input[type="text"][name="lastentry_from"]',
            'type': str,
            'example': '07/21/2020',
            'help': 'MM/DD/YYYY',
            'default': ''
        },
        'last_entry_to':{
            'kind': 'text',
            'selector': 'input[type="text"][name="lastentry_to"]',
            'type': str,
            'example': '07/21/2020',
            'help': 'MM/DD/YYYY',
            'default': ''
        },
        'nature_suit':{
            'kind': 'multiselect',
            'selector': 'select#nature_suit option',
            'type': str,
            'example': '110,152',
            'help': 'comma delimited codes e.g. "310,320"',
            'default': ''
        },
        'cause_action':{
            'kind': 'multiselect',
            'selector': 'select#cause_action option',
            'type': str,
            'example': '02:0431,02:0437',
            'help': 'comma delimited codes',
            'default': ''
        },
        'last_name':{
            'kind':'text',
            'selector': 'input[type="text"][name="last_name"]',
            'type': str,
            'example': 'Doe',
            'default': ''
        },
        'exact_match':{
            'kind': 'checkbox',
            'selector': 'input[type="checkbox"][name="ExactMatch"]',
            'type': bool,
            'example': True,
            'default': False
        },
        'first_name':{
            'kind': 'text',
            'selector': 'input[type="text"][name="first_name"]',
            'type': str,
            'example': 'Jane',
            'default': ''
        },
        'middle_name':{
            'kind': 'text',
            'selector': 'input[type="text"][name="middle_name"]',
            'type': str,
            'example': 'Janet',
            'default': ''
        },
        'person_type':{
            'kind': 'select',
            'selector': 'select#person_type',
            'type': str,
            'example': '8:16-cv-00750',
            'choices':['Attorney','Party', ''],
            'default': ''
        }
    },
    'buttons': {
        'submit': 'input[type="button"][value="Run Query"]',
        'find_this_case': 'input#case_number_find_button_0'
    },
    'pre_submit': lambda form: case_no_pre_submit(form)
}



def _clean_options_(el, first_only=True):
    ''' Clean the text of an Option WebElement '''
    try:
        if first_only:
            return el.text.strip().split()[0]
        else:
            return el.text.strip()
    except:
        return ''

def get_template(s):
    if s=='login':
        return TEMPLATE_LOGIN
    elif s=='query':
        return TEMPLATE_QUERY
    elif s=='docket':
        return TEMPLATE_DOCKET_SHEET

def fill_text(css_selector, value, browser):
    ''' Fill text foreccefully with JS, more reliable than WebDriver.send_keys,
    which doesn't work well if  field has an 'onchange' event that validates input '''
    if type(css_selector) is list:
        for sel in css_selector:
            try:
                browser.execute_script(f"document.querySelector('{sel}').value='{value}'")
            except:
                continue
        return
    browser.execute_script(f"document.querySelector('{css_selector}').value='{value}'")
    time.sleep(0.2)

def locator(css_selector, browser, get_many=False):
    ''' Locate an element by its CSS selector '''
    # If list of selectors supplied
    if type(css_selector) is list:
        for sel in css_selector:
            try:
                return browser.find_element(By.CSS_SELECTOR, sel)
            except:
                continue
        return

    if get_many:
        return browser.find_elements(By.CSS_SELECTOR, css_selector)
    else:
        try:
            return browser.find_element(By.CSS_SELECTOR, css_selector)
        except:
            return

def case_no_pre_submit(form):
    '''Hit enter on case number field to start the lookup'''

    if 'case_no' in form.fields:
        case_num = form.fields['case_no'].locate()
        case_num.send_keys(Keys.RETURN)


    run_button = form.buttons['submit'].locate()

    # Wait for the case lookup to run before you can 'Run Report'
    for i in range(5):
        if run_button.is_enabled():
            break
        # If the case selector appears, choose the first case (the main case)
        elif form.browser.find_element(By.ID, 'case_number_pick_area_0').is_displayed():
            # Check if any checkbox ticked
            docket_checkboxes = form.browser.find_elements(By.CSS_SELECTOR, '#case_number_pick_area_0 input[type="checkbox"]')
            if not any(box.is_selected() for box in docket_checkboxes):
                # Click the first if none pre-selected (default to main)
                docket_checkboxes[0].click()

        time.sleep(1)



class FormField:
    ''' Object that represents a single field as part of a form'''
    def __init__(self, name, kind, value, selector, browser):
        self.name = name
        self.kind = kind
        self.value = value
        self.selector = selector
        self.browser = browser

    def locate(self, get_many=False):
        ''' Returns the web element'''
        return locator(self.selector, self.browser, get_many)

    def fill(self):
        if self.kind == 'text':
            el = self.locate()
            try:
                el.clear()
            except:
                time.sleep(2)
                el = self.locate()
                el.clear()
            fill_text(self.selector, self.value, self.browser)
            # send_keys(el, self.value)

        elif self.kind == 'radio':
            chosen = [el for el in self.locate(get_many=True) \
                      if el.get_attribute('value')==self.value][0]
            chosen.click()

        elif self.kind == 'checkbox':
            current_state = bool(self.locate().get_property('checked'))
            if self.value != current_state:
                self.locate().click()

        # Selector for select should be for the select tag (not the options)
        elif self.kind == 'select':
            select_tag = self.locate()
            # Set the value manually which is same as what happens in fill_text
            fill_text(self.selector, self.value, self.browser)

        # Locator fn for select should return array of elements
        elif self.kind == 'multiselect':
            # Split out comma-delimited
            values = [str(v).strip() for v in self.value.split(',')]
            options = self.locate(get_many=True)
            subset = [el for el in options if _clean_options_(el) in values]
            for el in subset:
                el.click()

    def __repr__(self):
        return f'''<FormField: "{self.name}" (kind:{self.kind})>'''

class FormButton:
    def __init__(self, selector, browser):
        self.browser = browser
        self.selector = selector

    def locate(self, get_many=False):
        ''' Returns the web element'''
        return locator(self.selector, self.browser)

class FormFiller:
    ''' Object used to fill out a webpage form '''

    def __init__(self, browser, template, fill_values):
        '''
        Inputs:
            - browser: selenium browser driver
            - template ('query', 'login', 'docket' or dict): gets template from get_template, or else manual input as dict
            - fill_values (dict): key-value pairs of (field name, value to fill)
        '''
        self.fields, self.buttons = {}, {}
        self.browser = browser

        self.template = get_template(template) if type(template) is str else template
        self.build(fill_values)

    def build(self, fill_values):
        ''' Combine the template with the fill_values to create the FormFields'''
        for field_name, props in self.template['fields'].items():
            if field_name in fill_values:
                field = FormField(field_name, props['kind'], fill_values[field_name],\
                                  props['selector'], self.browser)
                self.fields[field_name] = field

        for button_name, selector in self.template['buttons'].items():
            self.buttons[button_name] = FormButton(selector, self.browser)

    def fill(self):
        ''' Fill all field values in the form'''
        for field in self.fields.values():
            field.fill()

        # If the form has a pre-submit method, execute it
        if 'pre_submit' in self.template:
            self.template['pre_submit'](self)

    def submit(self):
        ''' Click the form submit button '''
        self.buttons['submit'].locate().click()



def config_builder(tmp):
    '''
    CLI to generate a config file/dict
    Inputs
        - tmp (str or dict): name of template or else template dictionary obj
    Output
        - dict: field names and fill values for form  as k,v pairs
    '''
    template = get_template(tmp) if type(tmp) is str else template
    user_input = {}

    print("\n######\n## Form builder\n######\n")
    for key, attrs in template['fields'].items():

        #query form - case_no exludes all else
        if 'case_no' in user_input and tmp=='query':
            break

        pstring = f"\nEnter {key}"
        pstring += f" ({attrs['help']})" if attrs.get('help') else ''

        # Create choice variable if choice present
        var_type = click.Choice(attrs['choices']) if attrs.get('choices') else attrs['type']
        # Get the user input
        val = click.prompt(pstring, attrs['default'], type=var_type, show_choices=True)

        if val:
            user_input[key] = val

    while True:
        if click.confirm(f"\nDo you want to save this configuration?"):
            fpath = click.prompt("Filepath (.json)")
            fpath = std_path(fpath).resolve()
            if fpath.exists():
                print('File already exists')
            else:
                with open(fpath, 'w+', encoding="utf-8") as wfile:
                    json.dump(user_input, wfile, indent=2)
                break
        else:
            break
    return user_input
