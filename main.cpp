#include <iostream>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <atomic>

class TaskQueue {
private:
    std::queue<int> queue_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool finished_ = false;

public:
    void add(int value) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push(value);
        }
        cv_.notify_one(); // уведомляем один ожидающий поток
    }

    bool try_pop(int& value) {
        std::unique_lock<std::mutex> lock(mutex_);
        // Ждём, пока очередь не станет непустой или не будет сигнала завершения
        cv_.wait(lock, [this]() {
            return !queue_.empty() || finished_;
        });

        if (!queue_.empty()) {
            value = queue_.front();
            queue_.pop();
            return true;
        }
        return false;
    }

    void finish() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            finished_ = true;
        }
        cv_.notify_all(); // пробуждаем все потоки, чтобы они завершились
    }
};

void worker(int id, TaskQueue& taskQueue, std::atomic<int>& processedCount) {
    int value;
    while (taskQueue.try_pop(value)) {
        std::cout << "Поток " << id << " обработал задачу: " << value << std::endl;
        processedCount++;
        // Имитация обработки (не обязательна, но показывает асинхронность)
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    std::cout << "Поток " << id << " завершает работу." << std::endl;
}

int main() {
    const int NUM_WORKERS = 3; // Количество рабочих потоков

    TaskQueue taskQueue;
    std::vector<std::thread> workers;
    std::atomic<int> processedCount(0);

    // 1. Заполняем очередь задачами от 1 до 20
    for (int i = 1; i <= 20; ++i) {
        taskQueue.add(i);
    }

    // 2. Запускаем рабочие потоки
    for (int i = 0; i < NUM_WORKERS; ++i) {
        workers.emplace_back(worker, i + 1, std::ref(taskQueue), std::ref(processedCount));
    }

    // 3. Сигнализируем, что задачи больше добавляться не будут
    //    (В этой реализации завершение происходит после опустошения очереди,
    //     но корректно дождаться, когда все задачи обработаны — через finish)
    //     Однако по ТЗ: главный поток должен дождаться обработки всех задач,
    //     а затем завершить потоки. Проще: после запуска потоков ждём, пока
    //     processedCount не станет 20, затем вызываем finish.
    
    // Ожидаем, пока все 20 задач не будут обработаны
    while (processedCount.load() < 20) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Говорим очереди, что работа завершена (чтобы потоки вышли из try_pop)
    taskQueue.finish();

    // 4. Присоединяем потоки
    for (auto& t : workers) {
        if (t.joinable()) {
            t.join();
        }
    }

    std::cout << "Все задачи обработаны. Все потоки завершены." << std::endl;

    return 0;
}