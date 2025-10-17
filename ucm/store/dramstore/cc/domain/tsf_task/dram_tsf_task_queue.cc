/**
 * MIT License
 *
 * Copyright (c) 2025 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * */

#include "dram_tsf_task_queue.h"

namespace UC {

#define UC_DRAM_TASK_ERROR(s, t)                                                                   \
    do {                                                                                           \
        UC_ERROR("Failed({}) to run task({}).", (s), (t).taskId);                                  \
    } while (0)

Status DramTsfTaskQueue::Setup(const int32_t deviceId, DramTsfTaskSet* failureSet, MemoryPool* memPool)
{
    this->_failureSet = failureSet;
    this->_memPool = memPool;
    if (deviceId >= 0) {
        this->_device = DeviceFactory::Make(deviceId, 0, 0); // 这里不需要buffer，暂时都先传0吧
        if (!this->_device) { return Status::OutOfMemory(); }
    }
    if (!this->_streamOper.Setup([this](DramTsfTask& task) { this->StreamOper(task); },
            [this] { return this->_device->Setup().Success(); } )) {
        return Status::Error();
    }
    return Status::OK();
}

void DramTsfTaskQueue::Push(DramTsfTask& task)
{
    this->_streamOper.Push(task);
}

void DramTsfTaskQueue::StreamOper(DramTsfTask& task)
{
    if (this->_failureSet->Contains(task.taskId)) {
        this->Done(task, false);
        return;
    }
    if (task.type == DramTsfTask::Type::LOAD) {
        this->H2D(task);
    } else {
        this->D2H(task);
    }
}

// 这个H2D和D2H函数是重点要重新实现的。
void DramTsfTaskQueue::H2D(DramTsfTask& task)
{
    // TODO 这里地址要重新写逻辑
    // auto block_addr = this->_memPool->GetAddress(task.blockId);
    // auto host_src = block_addr + task.offset;
    // if (!host_src) {
    //     UC_DRAM_TASK_ERROR(Status::Error(), task);
    //     this->Done(task, false);
    //     return;
    // }

    uintptr_t host[task.number] = {0};
    uintptr_t dev[task.number] = {0};

    for (auto& shard : task.shards) {
        auto idx = shard.index;
        auto block_addr = this->_memPool->GetAddress(shard.block);
        auto host_src = block_addr + shard.offset;
        if (!host_src) {
            UC_DRAM_TASK_ERROR(Status::Error(), task);
            this->Done(task, false);
            return;
        }
        host[idx] = (uintptr_t)host_src;
        dev[idx] = shard.address;
    }

    // auto status = this->_device->H2DAsync((std::byte*)task.address, (std::byte*)host_src, task.length);
    auto status = this->_device->H2DBatch(host, dev, task.number, task.size);

    if (status.Failure()) {
        UC_DRAM_TASK_ERROR(status, task);
        this->Done(task, false);
        return;
    }
    status = this->_device->AppendCallback([this, task](bool success) mutable {
        if (!success) { UC_DRAM_TASK_ERROR(Status::Error(), task); }
        this->Done(task, success);
        // 这里是否需要return？
    });
    if (status.Failure()) {
        UC_DRAM_TASK_ERROR(status, task);
        this->Done(task, false);
        return;
    }
}

// 这个函数也是重点要重新实现的。
void DramTsfTaskQueue::D2H(DramTsfTask& task)
{
    // // TODO 这里地址要重新写逻辑
    // auto block_addr = this->_memPool->GetAddress(task.blockId);
    // if (!block_addr) {
    //     // 如果还没有，那么临时分配
    //     this->_memPool->NewBlock(task.blockId);
    //     block_addr = this->_memPool->GetAddress(task.blockId);
    //     if (!block_addr) {
    //         UC_DRAM_TASK_ERROR(Status::Error(), task);
    //         this->Done(task, false);
    //         return;
    //     }
    // }
    // auto host_dst = block_addr + task.offset;
    // if (!host_dst) {
    //     UC_DRAM_TASK_ERROR(Status::Error(), task);
    //     this->Done(task, false);
    //     return;
    // }

    uintptr_t host[task.number] = {0};
    uintptr_t dev[task.number] = {0};

    for (auto& shard : task.shards) {
        auto idx = shard.index;
        auto block_addr = this->_memPool->GetAddress(shard.block);
        auto host_src = block_addr + shard.offset;
        if (!host_src) {
            UC_DRAM_TASK_ERROR(Status::Error(), task);
            this->Done(task, false);
            return;
        }
        host[idx] = (uintptr_t)host_src;
        dev[idx] = shard.address;
    }

    // auto status = this->_device->D2HAsync((std::byte*)host_dst, (std::byte*)task.address, task.length);
    auto status = this->_device->D2HBatch(host, dev, task.number, task.size);

    if (status.Failure()) {
        UC_DRAM_TASK_ERROR(status, task);
        this->Done(task, false);
        return;
    }
    status = this->_device->AppendCallback([this, task](bool success) mutable {
        if (!success) {
            UC_DRAM_TASK_ERROR(Status::Error(), task);
            this->Done(task, false);
            return; // 这里是否需要return？
        }
        this->Done(task, true);
    });
    if (status.Failure()) {
        UC_DRAM_TASK_ERROR(status, task);
        this->Done(task, false);
        return;
    }
}

void DramTsfTaskQueue::Done(const DramTsfTask& task, bool success)
{
    if (!success) { this->_failureSet->Insert(task.taskId); }
    task.waiter->Done();
}

} // namespace UC
