import torch
import torch.nn as nn
import torch.nn.functional as F


class BinaryCrossEntropy(nn.Module):
    def __init__(self, model, weight=None, **kwargs):
        super().__init__()

    def forward(self, input, target):
        try:
            loss = F.binary_cross_entropy(input.view(-1), target.view(-1))
        except RuntimeError:
            raise RuntimeError(f"Error caught for input {input} and target: {target}")
        return loss


class MSELoss(nn.Module):
    def __init__(self, model, weight=None, **kwargs):
        super().__init__()

    def forward(self, input, target):
        return F.mse_loss(input.view(-1), target.view(-1), reduction='mean')


class MSLELoss(nn.Module):
    def __init__(self, model, **kwargs):
        self.threshold = 1
        super().__init__()

    def forward(self, input, target):
        # When the input is <= -t, the log will be negative and the loss will be NaN.
        # In these cases, value will be set to -0.9 threshold, so that the loss is defined while maintaining the strong error
        # The same holds true for target, but this should anyway not be negative.

        input = input.clip(min=-0.9 * self.threshold)
        target = target.clip(min=-0.9 * self.threshold)

        thresholds = torch.full((len(target), 1), self.threshold, device=target.device)
        input_added = torch.add(input, thresholds)
        target_added = torch.add(target, thresholds)

        input_logs = torch.log10(input_added)
        target_logs = torch.log10(target_added)

        diff = torch.sub(input_logs, target_logs)
        squared = torch.square(diff)
        loss = torch.mean(squared)
        return loss


class QLoss(nn.Module):
    def __init__(self, model, weight=None, min_val=0.0001, penalty_negative=1e3, **kwargs):
        self.min_val = min_val
        self.penalty_negative = penalty_negative
        super().__init__()

    def forward(self, input, target):
        input_zero_mask = input == 0
        target_zero_mask = target == 0

        if input_zero_mask.any():
            input = input + input_zero_mask * torch.full(input.shape, 0.0000001, device=input.device)
        if target_zero_mask.any():
            target = target + target_zero_mask * torch.full(input.shape, 0.0000001, device=target.device)

        q_error = torch.zeros((len(target), 1), device=target.device)

        # create mask for entries which should be penalized for negative/too small estimates
        penalty_mask = input < self.min_val
        inverse_penalty_mask = input >= self.min_val
        q_error_penalties = torch.mul(1 - input, penalty_mask) * self.penalty_negative

        # influence on loss for a negative estimate is >= penalty_negative constant
        q_error = torch.add(q_error, q_error_penalties)

        # calculate normal q error for other instances
        input_masked = torch.mul(input, inverse_penalty_mask)
        target_masked = torch.mul(target.reshape((-1, 1)), inverse_penalty_mask)

        q_error = torch.add(q_error, torch.max(torch.div(input_masked, target.reshape((-1, 1))),
                                               torch.div(target_masked, input)))

        loss = torch.mean(q_error)
        return loss