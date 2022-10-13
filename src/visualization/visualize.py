import matplotlib.pyplot as plt
import seaborn as sns

def plot_na_by_col(df, **kwargs):
    na_by_col = df.isna().sum().compute() / len(df) 
    na_by_col.sort_values(ascending=False, inplace=True)
    fig, ax = plt.subplots(**kwargs)
    ax = sns.barplot(
        x=na_by_col,
        y=na_by_col.index,
        color='red'
    )
    for i in ax.containers:
        ax.bar_label(i, fmt='%.2f')
    plt.ylabel('variable')
    plt.xlabel('% missing')
    plt.show()