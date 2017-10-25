""" this module is used as a global context across multiple layouts and filters """

# Constants that are initialized once when the application starts and will not be changed during runtime
MATCH = "Yes"
ALL = "All"
NOT_LABELED = "Not-Labeled"
NON_MATCH = "Not-Matched"
NOT_SURE = "Not-Sure"
ALL_ATTRIBUTES = None
DEFAULT_TUPLE_PAIR_COUNT_PER_PAGE = 5
SAVEPATH = "./"
DEFAULT_SAVE_FILE_NAME = "default_save_file"
PAGE_DISPLAY_COUNT = 6
COMMENTS_COLUMN = "comments"
TAGS_COLUMN = "tags"
LABEL_COLUMN = "label"
COMPLETE_DATA_FRAME = None
TOTAL_NUMBER_OF_TUPLE_PAIRS = 0
VALID_LAYOUTS = ('horizontal', 'vertical', 'single')
VALID_LABELS = (MATCH, NON_MATCH, NOT_LABELED, NOT_SURE)
# Application values that change during runtime. These are used for operations such as:
# - Filtering attributes to be displayed
# - Change display layout
# - Change the number of tuple pairs displayed
# - Keep track of the current page number being displayed
# - Keep track of current save file name
# - Keep track of data with current attribute filters applied to it
# - Keep track of the number of alphabets shown per attribute
tuple_pair_count_per_page = 5
current_page_number = 0
current_layout = 'horizontal'
alphabets_per_attribute_display = 5
save_file_name = DEFAULT_SAVE_FILE_NAME
current_data_frame = None
current_attributes = None

# Controllers:
# todo 5/4/17 a different approach - better imports - than maintaining in context
FILTER_CONTROLLER = None
STATS_CONTROLLER = None
TUPLE_PAIR_DISPLAY_CONTROLLER = None
LABEL_CONTROLLER = None

# todo 4/26/17 can these values linger if app crashes??
