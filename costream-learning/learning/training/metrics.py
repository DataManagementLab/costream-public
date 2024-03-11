import copy
import numpy as np
from sklearn.metrics import mean_squared_error, f1_score, recall_score, accuracy_score


class Metric:
    def __init__(self, metric_prefix=None, metric_name='metric', maximize=True, early_stopping_metric=False):
        self.maximize = maximize
        self.default_value = -np.inf
        if not self.maximize:
            self.default_value = np.inf
        self.best_seen_value = self.default_value
        self.last_seen_value = self.default_value
        if metric_prefix:
            self.metric_name = metric_prefix + "_" + metric_name
        else:
            self.metric_name = metric_name
        self.best_model = None
        self.early_stopping_metric = early_stopping_metric

    def evaluate(self, model=None, metrics_dict=None, **kwargs):
        metric = self.default_value
        try:
            metric = self.evaluate_metric(**kwargs)
        except ValueError as e:
            print(f"Observed ValueError in metrics calculation {e}")
        self.last_seen_value = metric

        metrics_dict[self.metric_name] = metric
        print(f"{self.metric_name}: \t {metric:.4f} \t [best: {self.best_seen_value:.4f}]")

        best_seen = False
        if (self.maximize and metric > self.best_seen_value) or (not self.maximize and metric < self.best_seen_value):
            self.best_seen_value = metric
            best_seen = True
            if model is not None:
                self.best_model = copy.deepcopy(model.state_dict())
        return best_seen


class MAPE(Metric):
    def __init__(self, **kwargs):
        super().__init__(metric_name='mape', maximize=False, **kwargs)

    def evaluate_metric(self, labels=None, preds=None, probs=None):
        mape = np.mean(np.abs((labels - preds) / labels))
        return mape

    def evaluate_metric(self, labels=None, preds=None):
        raise NotImplementedError


class RMSE(Metric):
    def __init__(self, **kwargs):
        super().__init__(metric_name='rmse', maximize=False, **kwargs)

    def evaluate_metric(self, labels=None, preds=None, probs=None):
        val_mse = np.sqrt(mean_squared_error(labels, preds))
        return val_mse


class MAPE(Metric):
    def __init__(self, **kwargs):
        super().__init__(metric_name='mape', maximize=False, **kwargs)

    def evaluate_metric(self, labels=None, preds=None, probs=None):
        mape = np.mean(np.abs((labels - preds) / labels))
        return mape


class QError(Metric):
    def __init__(self, percentile=50, min_val=0.1, **kwargs):
        super().__init__(metric_name=f'q_error_{percentile}', maximize=False, **kwargs)
        self.percentile = percentile
        self.min_val = min_val

    def evaluate_metric(self, labels=None, preds=None, probs=None):
        # assert np.all(labels >= self.min_val) todo
        preds = np.abs(preds)
        labels = np.abs(labels)
        # preds = np.clip(preds, self.min_val, np.inf)

        q_errors = np.maximum(labels / preds, preds / labels)
        q_errors = np.nan_to_num(q_errors, nan=np.inf)
        median_q = np.percentile(q_errors, self.percentile)
        return median_q


class Accuracy(Metric):
    def __init__(self, **kwargs):
        super().__init__(metric_name=f'accuracy', maximize=True, **kwargs)

    def evaluate_metric(self, labels=None, preds=None, probs=None):
        round_preds = np.round(preds)
        equality_mask = labels == round_preds
        score = np.count_nonzero(equality_mask) / len(equality_mask)
        assert accuracy_score(labels, round_preds) == score
        return score


class F1Score(Metric):
    def __init__(self, **kwargs):
        super().__init__(metric_name=f'f1-score', maximize=True, **kwargs)

    def evaluate_metric(self, labels=None, preds=None, probs=None):
        round_preds = np.round(preds)
        f1 = f1_score(labels, round_preds)
        return f1


class Recall(Metric):
    def __init__(self, **kwargs):
        super().__init__(metric_name=f'recall', maximize=True, **kwargs)

    def evaluate_metric(self, labels=None, preds=None, probs=None):
        round_preds = np.round(preds)
        recall = recall_score(labels, round_preds)
        return recall
