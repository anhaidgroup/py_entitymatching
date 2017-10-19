from jinja2 import Environment, PackageLoader, select_autoescape
from math import ceil

from py_entitymatching.labeler.new_labeler.utils import ApplicationContext

# load all templates from 'view' package and 'templates' folder
env = Environment(
    loader=PackageLoader('py_entitymatching.labeler.new_labeler.view', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)


def render_options_page(tags_col, comments_col):
    """ Renders page asking user to enter/validate column names for tags and comments.
    
    Args:
        tags_col (str): Suggestion for the name of the tags column.
        comments_col (str): Suggestion for the name of the comments column.
        
    Returns:
        Options page HTML (str).
    
    Raises:
    """
    options_page = env.get_template('options.html')
    return options_page.render(tags_col=tags_col, comments_col=comments_col)


def render_tuple_pair(tuple_pair):
    """ Renders a single tuple pair using template tuple_pair.html.

    Args:
        tuple_pair : Single tuple pair to be rendered.
        
    Returns:
        Tuple pair HTML (str).
        
    Raises:
    """
    tuple_pair_template = env.get_template('tuple_pair.html');
    # todo 5/7/17 check tuple pair to be 1 row only
    return tuple_pair_template.render(row=tuple_pair.to_dict(orient='records'), headers=['ID', 'birth_year', 'name']);


def compute_page_numbers(current_page):
    """ Utility method to compute start and end page numbers to show in pagination footer.

    Args:
        current_page (int): Page number of the current page being displayed.
        
    Returns:
        [start_page, end_page] (int, int): Starting page number and ending page number to be 
        displayed in pagination footer.
        
    Raises:
    """
    if current_page - ApplicationContext.PAGE_DISPLAY_COUNT / 2 < 0:
        start_page = 0
        end_page = ApplicationContext.PAGE_DISPLAY_COUNT
    else:
        start_page = current_page - ApplicationContext.PAGE_DISPLAY_COUNT // 2
        end_page = start_page + ApplicationContext.PAGE_DISPLAY_COUNT
    return [start_page, end_page]


def render_main_page(current_page_tuple_pairs, match_count, not_match_count, not_sure_count, unlabeled_count):
    """ Renders complete main window based on current layout.

    Args:
        current_page_tuple_pairs (DataFrame): Tuple pairs to be displayed in the current page
        match_count (int):
        not_match_count (int):
        not_sure_count (int):
        unlabeled_count (int):
        
    Returns: Complete window HTML (str)
    
    Raises:
    """
    template = env.get_template('horizontal_layout.html')
    if ApplicationContext.current_layout == "horizontal":
        template = env.get_template('horizontal_layout.html')
    elif ApplicationContext.current_layout == "vertical":
        template = env.get_template('vertical_layout.html')
    elif ApplicationContext.current_layout == "single":
        template = env.get_template('single_layout.html')

    [start_page_number, end_page_number] = compute_page_numbers(ApplicationContext.current_page_number)
    return template.render(layout=ApplicationContext.current_layout, tuple_pairs=current_page_tuple_pairs.to_dict(orient='records'),
                           attributes=ApplicationContext.current_attributes,
                           count_per_page=ApplicationContext.tuple_pair_count_per_page,
                           number_of_pages=ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_number_of_pages(
                               ApplicationContext.current_data_frame),
                           start_page_number=start_page_number, end_page_number=ApplicationContext.TUPLE_PAIR_DISPLAY_CONTROLLER.get_number_of_pages(
            ApplicationContext.current_data_frame),
                           current_page=ApplicationContext.current_page_number, match_count=match_count,
                           not_match_count=not_match_count, not_sure_count=not_sure_count,
                           unlabeled_count=unlabeled_count, total_count=ApplicationContext.COMPLETE_DATA_FRAME.shape[0],
                           tokens_per_attribute=ApplicationContext.alphabets_per_attribute_display,
                           save_file_name=ApplicationContext.save_file_name,
                           comments_col=ApplicationContext.COMMENTS_COLUMN, tags_col=ApplicationContext.TAGS_COLUMN,
                           label_column_name=ApplicationContext.LABEL_COLUMN)
