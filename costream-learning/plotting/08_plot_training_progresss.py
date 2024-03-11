import argparse
import pandas as pd
import matplotlib.pyplot as plt
from learning.constants import LABEL

parser = argparse.ArgumentParser()
parser.add_argument('--dataset_path', default=None, required=True)
parser.add_argument('--metrics', default=None, required=True, choices=[LABEL.ELAT, LABEL.TPT], nargs='+')
args = parser.parse_args()

for m in args.metrics:
    df = pd.read_csv(args.dataset_path + "/models/model/model-"+m+".csv")
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(5, 5))

    # MSE
    ax1.set_xlabel('Epochs')
    ax1.set_yscale('log')
    ax1.plot(df.epoch, df.val_loss, color="red", label="Validation Loss")
    ax1.plot(df.epoch, df.mean_loss, color="green", label="Train Loss")
    ax1.legend()

    # Median Q-Error of validation set
    ax2.set_xlabel('Epochs')
    ax2.plot(df.epoch, df.val_median_q_error_50, color="blue", label="Val 50% Q-Error")
    ax2.plot(df.epoch, df.val_median_q_error_95, color="orange", label="Val 95%. Q-Error")
    ax2.set_yscale('log')
    ax2.legend()

    # 95th percentile of Q-error of validation set
    ax3.set_xlabel('Epochs')
    ax3.plot(df.epoch, df.val_mse, color="purple", label="Val MSE")
    ax3.set_yscale('log')
    ax3.legend()

    plt.suptitle(str(m).capitalize())
    fig.tight_layout()
    plt.show()
