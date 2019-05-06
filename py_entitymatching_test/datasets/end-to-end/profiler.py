import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# def profile_table(df, attribute, plot=True):
#     out_df = pd.DataFrame(columns=['Property', 'Value'])
#     unique_values = pd.unique(df[attribute])
#     num_missing = sum(pd.isnull(df[attribute]))
#
#     if not plot:
#         out_df.set_value(0, 'Property', 'Num. Missing Values')
#         out_df.set_value(0, 'Value', num_missing)
#         out_df.set_value(1, 'Property', 'Num. Unique Values')
#         out_df.set_value(1, 'Value', len(unique_values))
#         out_df.set_value(2, 'Property', 'List of Unique Values')
#         out_df.set_value(2, 'Value', sorted(list(unique_values)))
#         return out_df
#     else:
#         print('Number of unique values: %d' % len(unique_values))
#         print('Number of missing values: %d' % num_missing)
#         print('\nUnique values: ')
#         print(sorted(list(unique_values)))
#         print('\nFrequency plot:\n')
#
#         d = (pd.DataFrame(df[attribute].value_counts()))
#         d.sort_index(inplace=True)
#         ax = sns.barplot(x="index", y=attribute, data=(
#         pd.DataFrame(df[attribute].value_counts())).reset_index())
#         ax.set(xlabel=attribute, ylabel='count')
#         ax.grid(b=True, which='major', color='w', linewidth=1.0)
#         ax.set_xticklabels(labels=d.index.values, rotation=90)
#         plt.show()


def profile_table(df, attribute, plot=True):

    unique_values = pd.unique(df[attribute])
    num_missing = sum(pd.isnull(df[attribute]))

    if not plot:
        return  pd.DataFrame({'Property':['Num. Missing Values', 'Num. Unique Values', 'List of Unique Values'],
                               'Value':[num_missing, len(unique_values), sorted(list(unique_values))]})
    else:
        print('Number of unique values: %d\nNumber of missing values: '
              '%d\n\nUnique values:'  % (len(unique_values), num_missing))
        print(sorted(list(unique_values)))
        print('\nFrequency plot:\n')
        d = (pd.DataFrame(df[attribute].value_counts()))
        ax = sns.barplot(x="index", y=attribute, data=(d).reset_index())
        ax.set(xlabel=attribute, ylabel='count')
        ax.grid(b=True, which='major', color='w', linewidth=1.0)
        ax.set_xticklabels(labels=d.sort_index().index.values, rotation=90)
        plt.show()
